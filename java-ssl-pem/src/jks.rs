//! 自研最小 JKS 读取：truststore 证书 + client 私钥解密（不依赖外部 `jks` crate）。
//!
//! 格式对齐 OpenJDK `JavaKeyStore` / `KeyProtector`（OID `1.3.6.1.4.1.42.2.17.1.1`）。

use sha1::{Digest, Sha1};

use crate::JavaSslPemError;

const MAGIC: u32 = 0xfeed_feed;
const VERSION_1: u32 = 1;
const VERSION_2: u32 = 2;
const TAG_PRIVATE_KEY: u32 = 1;
const TAG_TRUSTED_CERT: u32 = 2;
const WHITENER: &[u8] = b"Mighty Aphrodite";
const SALT_LEN: usize = 20;
/// EncryptedPrivateKeyInfo 内算法 OID：`1.3.6.1.4.1.42.2.17.1.1`
const KEY_PROTECTOR_OID: &[u8] = &[0x2b, 0x06, 0x01, 0x04, 0x01, 0x2a, 0x02, 0x11, 0x01, 0x01];

#[derive(Debug, Clone)]
pub(crate) struct JksCert {
    pub cert_type: String,
    pub der: Vec<u8>,
}

#[derive(Debug)]
enum JksEntry {
    TrustedCert(JksCert),
    PrivateKey {
        /// EncryptedPrivateKeyInfo DER（尚未解密）
        encrypted_pkcs8: Vec<u8>,
        chain: Vec<JksCert>,
    },
}

#[derive(Debug)]
pub(crate) struct JksStore {
    /// `(alias, entry)`，保持文件顺序
    entries: Vec<(String, JksEntry)>,
}

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
    hasher: Sha1,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            hasher: Sha1::new(),
        }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn read_exact(&mut self, n: usize) -> Result<&'a [u8], JavaSslPemError> {
        if self.remaining() < n {
            return Err(JavaSslPemError::Jks {
                role: "parse",
                path: String::new(),
                detail: format!("truncated JKS (need {n} bytes, have {})", self.remaining()),
            });
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        self.hasher.update(slice);
        Ok(slice)
    }

    fn read_u16(&mut self) -> Result<u16, JavaSslPemError> {
        let b = self.read_exact(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, JavaSslPemError> {
        let b = self.read_exact(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, JavaSslPemError> {
        let b = self.read_exact(8)?;
        Ok(u64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn read_utf(&mut self) -> Result<String, JavaSslPemError> {
        let len = self.read_u16()? as usize;
        let raw = self.read_exact(len)?;
        String::from_utf8(raw.to_vec()).map_err(|e| JavaSslPemError::Jks {
            role: "parse",
            path: String::new(),
            detail: format!("invalid UTF-8 alias/type: {e}"),
        })
    }

    fn read_cert(&mut self, version: u32) -> Result<JksCert, JavaSslPemError> {
        let cert_type = match version {
            VERSION_1 => "X509".to_string(),
            VERSION_2 => self.read_utf()?,
            other => {
                return Err(JavaSslPemError::Jks {
                    role: "parse",
                    path: String::new(),
                    detail: format!("unsupported JKS version {other}"),
                });
            }
        };
        let len = self.read_u32()? as usize;
        let der = self.read_exact(len)?.to_vec();
        Ok(JksCert { cert_type, der })
    }
}

/// JKS 口令字节：每个 UTF-8 字节前插 `0`（ASCII 与 Java UTF-16BE 一致）。
fn password_bytes(password: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(password.len() * 2);
    for &b in password.as_bytes() {
        out.push(0);
        out.push(b);
    }
    out
}

fn read_der_length(data: &[u8]) -> Result<(usize, usize), JavaSslPemError> {
    if data.is_empty() {
        return Err(JavaSslPemError::Jks {
            role: "key",
            path: String::new(),
            detail: "EncryptedPrivateKeyInfo: empty DER length".into(),
        });
    }
    let first = data[0];
    if first & 0x80 == 0 {
        return Ok((first as usize, 1));
    }
    let n = (first & 0x7f) as usize;
    if n == 0 || n > 4 || data.len() < 1 + n {
        return Err(JavaSslPemError::Jks {
            role: "key",
            path: String::new(),
            detail: "EncryptedPrivateKeyInfo: bad DER length".into(),
        });
    }
    let mut len = 0usize;
    for i in 0..n {
        len = (len << 8) | data[1 + i] as usize;
    }
    Ok((len, 1 + n))
}

fn derive_xor_key(salt: &[u8], pwd16: &[u8], length: usize) -> Vec<u8> {
    let digest_size = 20;
    let rounds = (length + digest_size - 1) / digest_size;
    let mut xor_key = vec![0u8; length];
    let mut digest = salt.to_vec();
    for i in 0..rounds {
        let mut h = Sha1::new();
        h.update(pwd16);
        h.update(&digest);
        digest = h.finalize().to_vec();
        let offset = i * digest_size;
        let n = (length - offset).min(digest_size);
        xor_key[offset..offset + n].copy_from_slice(&digest[..n]);
    }
    xor_key
}

/// 解密 JKS `EncryptedPrivateKeyInfo` → PKCS#8 DER。
fn decrypt_private_key(epki: &[u8], password: &str) -> Result<Vec<u8>, JavaSslPemError> {
    if epki.is_empty() || epki[0] != 0x30 {
        return Err(JavaSslPemError::Jks {
            role: "key",
            path: String::new(),
            detail: "EncryptedPrivateKeyInfo: not a SEQUENCE".into(),
        });
    }
    let mut pos = 1;
    let (seq_len, n) = read_der_length(&epki[pos..])?;
    pos += n;
    if pos + seq_len > epki.len() {
        return Err(JavaSslPemError::Jks {
            role: "key",
            path: String::new(),
            detail: "EncryptedPrivateKeyInfo: truncated".into(),
        });
    }

    if epki.get(pos) != Some(&0x30) {
        return Err(JavaSslPemError::Jks {
            role: "key",
            path: String::new(),
            detail: "EncryptedPrivateKeyInfo: expected AlgorithmIdentifier".into(),
        });
    }
    pos += 1;
    let (algo_len, n) = read_der_length(&epki[pos..])?;
    pos += n;
    let algo_end = pos + algo_len;

    if epki.get(pos) != Some(&0x06) {
        return Err(JavaSslPemError::Jks {
            role: "key",
            path: String::new(),
            detail: "EncryptedPrivateKeyInfo: expected OID".into(),
        });
    }
    pos += 1;
    let oid_len = *epki.get(pos).ok_or_else(|| JavaSslPemError::Jks {
        role: "key",
        path: String::new(),
        detail: "EncryptedPrivateKeyInfo: truncated OID".into(),
    })? as usize;
    pos += 1;
    let oid = epki.get(pos..pos + oid_len).ok_or_else(|| JavaSslPemError::Jks {
        role: "key",
        path: String::new(),
        detail: "EncryptedPrivateKeyInfo: truncated OID body".into(),
    })?;
    if oid != KEY_PROTECTOR_OID {
        return Err(JavaSslPemError::Jks {
            role: "key",
            path: String::new(),
            detail: "unsupported key protection OID (need Java KeyProtector)".into(),
        });
    }
    pos = algo_end;

    if epki.get(pos) != Some(&0x04) {
        return Err(JavaSslPemError::Jks {
            role: "key",
            path: String::new(),
            detail: "EncryptedPrivateKeyInfo: expected OCTET STRING".into(),
        });
    }
    pos += 1;
    let (octet_len, n) = read_der_length(&epki[pos..])?;
    pos += n;
    let key_data = epki.get(pos..pos + octet_len).ok_or_else(|| JavaSslPemError::Jks {
        role: "key",
        path: String::new(),
        detail: "EncryptedPrivateKeyInfo: truncated ciphertext".into(),
    })?;

    if key_data.len() < SALT_LEN + 20 {
        return Err(JavaSslPemError::Jks {
            role: "key",
            path: String::new(),
            detail: "EncryptedPrivateKeyInfo: ciphertext too short".into(),
        });
    }
    let salt = &key_data[..SALT_LEN];
    let enc_len = key_data.len() - SALT_LEN - 20;
    let encrypted = &key_data[SALT_LEN..SALT_LEN + enc_len];
    let stored_digest = &key_data[SALT_LEN + enc_len..];

    let pwd16 = password_bytes(password);
    let xor_key = derive_xor_key(salt, &pwd16, enc_len);
    let mut plain = vec![0u8; enc_len];
    for i in 0..enc_len {
        plain[i] = encrypted[i] ^ xor_key[i];
    }

    let mut h = Sha1::new();
    h.update(&pwd16);
    h.update(&plain);
    if h.finalize().as_slice() != stored_digest {
        return Err(JavaSslPemError::Jks {
            role: "key",
            path: String::new(),
            detail: "private key password mismatch (KeyProtector checksum failed)".into(),
        });
    }
    Ok(plain)
}

impl JksStore {
    pub(crate) fn load(data: &[u8], password: &str) -> Result<Self, JavaSslPemError> {
        let mut c = Cursor::new(data);
        let pwd16 = password_bytes(password);
        c.hasher.update(&pwd16);
        c.hasher.update(WHITENER);

        let magic = c.read_u32()?;
        if magic != MAGIC {
            return Err(JavaSslPemError::Jks {
                role: "load",
                path: String::new(),
                detail: format!("bad magic {magic:#x} (expected 0xfeedfeed)"),
            });
        }
        let version = c.read_u32()?;
        if version != VERSION_1 && version != VERSION_2 {
            return Err(JavaSslPemError::Jks {
                role: "load",
                path: String::new(),
                detail: format!("unsupported JKS version {version}"),
            });
        }
        let count = c.read_u32()? as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let tag = c.read_u32()?;
            let alias = c.read_utf()?;
            let entry = match tag {
                TAG_PRIVATE_KEY => {
                    let _ts = c.read_u64()?;
                    let len = c.read_u32()? as usize;
                    let encrypted_pkcs8 = c.read_exact(len)?.to_vec();
                    let n_certs = c.read_u32()? as usize;
                    let mut chain = Vec::with_capacity(n_certs);
                    for _ in 0..n_certs {
                        chain.push(c.read_cert(version)?);
                    }
                    JksEntry::PrivateKey {
                        encrypted_pkcs8,
                        chain,
                    }
                }
                TAG_TRUSTED_CERT => {
                    let _ts = c.read_u64()?;
                    JksEntry::TrustedCert(c.read_cert(version)?)
                }
                other => {
                    return Err(JavaSslPemError::Jks {
                        role: "parse",
                        path: String::new(),
                        detail: format!("unknown entry tag {other}"),
                    });
                }
            };
            entries.push((alias, entry));
        }

        let computed = c.hasher.clone().finalize();
        if c.remaining() < 20 {
            return Err(JavaSslPemError::Jks {
                role: "load",
                path: String::new(),
                detail: "missing store integrity digest".into(),
            });
        }
        let stored = &c.data[c.pos..c.pos + 20];
        if computed.as_slice() != stored {
            return Err(JavaSslPemError::Jks {
                role: "load",
                path: String::new(),
                detail: "store password mismatch (integrity digest failed)".into(),
            });
        }
        Ok(Self { entries })
    }

    pub(crate) fn trusted_certs(&self) -> impl Iterator<Item = (&str, &JksCert)> {
        self.entries.iter().filter_map(|(alias, e)| match e {
            JksEntry::TrustedCert(c) => Some((alias.as_str(), c)),
            _ => None,
        })
    }

    pub(crate) fn private_key_aliases(&self) -> Vec<String> {
        self.entries
            .iter()
            .filter_map(|(alias, e)| match e {
                JksEntry::PrivateKey { .. } => Some(alias.clone()),
                _ => None,
            })
            .collect()
    }

    /// 按别名取私钥（大小写不敏感）并解密；`chain` 为证书 DER 列表。
    pub(crate) fn decrypt_private_key(
        &self,
        alias: &str,
        password: &str,
    ) -> Result<(Vec<u8>, Vec<JksCert>), JavaSslPemError> {
        let want = alias.to_lowercase();
        let entry = self
            .entries
            .iter()
            .find(|(a, _)| a.to_lowercase() == want)
            .map(|(_, e)| e)
            .ok_or_else(|| JavaSslPemError::Jks {
                role: "keystore",
                path: String::new(),
                detail: format!("alias {alias:?} not found"),
            })?;
        match entry {
            JksEntry::PrivateKey {
                encrypted_pkcs8,
                chain,
            } => {
                let key = decrypt_private_key(encrypted_pkcs8, password)?;
                Ok((key, chain.clone()))
            }
            JksEntry::TrustedCert(_) => Err(JavaSslPemError::Jks {
                role: "keystore",
                path: String::new(),
                detail: format!("alias {alias:?} is a trusted certificate, not a private key"),
            }),
        }
    }
}
