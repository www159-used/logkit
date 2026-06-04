//! 将分支权重在**构造时**整理为 [`BranchPicker`]，运行时只做 O(1) 抽样。
//!
//! 使用 [`rand_distr::WeightedIndex`]（别名表 / Walker 风格），
//! 比每轮对 `w` 做线性扫描更稳，分支数较多时更合适。

use rand::distributions::Distribution;
use rand::thread_rng;
use rand_distr::WeightedError;
use rand_distr::WeightedIndex;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BranchError {
    #[error("branch list must not be empty")]
    Empty,
    #[error("branch weight must be > 0, got {0} at index {1}")]
    ZeroWeight(u32, usize),
    #[error("invalid weights: {0}")]
    Invalid(#[from] WeightedError),
}

/// 编译期（配置加载 / `into_slot`）构建的加权分支选择器。
#[derive(Debug, Clone)]
pub struct BranchPicker {
    index: WeightedIndex<u32>,
    len: usize,
}

impl BranchPicker {
    /// `weights[i]` 为第 `i` 个分支的权重，须全部 > 0。
    pub fn new(weights: &[u32]) -> Result<Self, BranchError> {
        if weights.is_empty() {
            return Err(BranchError::Empty);
        }
        for (i, &w) in weights.iter().enumerate() {
            if w == 0 {
                return Err(BranchError::ZeroWeight(w, i));
            }
        }
        let len = weights.len();
        let index = WeightedIndex::new(weights).map_err(BranchError::Invalid)?;
        Ok(Self { index, len })
    }

    /// 按权重随机返回分支下标。
    pub fn choose(&self) -> usize {
        self.index.sample(&mut thread_rng())
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_and_zero_weight() {
        assert!(matches!(BranchPicker::new(&[]), Err(BranchError::Empty)));
        assert!(matches!(
            BranchPicker::new(&[1, 0, 1]),
            Err(BranchError::ZeroWeight(0, 1))
        ));
    }

    #[test]
    fn heavy_branch_wins_more_often() {
        let picker = BranchPicker::new(&[1, 9]).unwrap();
        let mut hits = [0usize; 2];
        for _ in 0..10_000 {
            hits[picker.choose()] += 1;
        }
        assert!(hits[1] > hits[0] * 3, "hits={hits:?}");
    }
}
