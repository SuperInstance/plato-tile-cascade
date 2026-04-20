//! Dependency cascade engine — propagate updates, invalidations, and revalidations downstream.
//! Part of the PLATO framework.

use std::collections::{HashMap, HashSet, VecDeque};

/// A directed dependency graph edge.
#[derive(Debug, Clone)]
pub struct Dependency {
    pub from: String, // upstream tile id
    pub to: String,   // downstream tile id
}

/// Cascade event types.
#[derive(Debug, Clone, PartialEq)]
pub enum CascadeEvent {
    Updated { tile_id: String },
    Invalidated { tile_id: String, reason: String },
    Revalidated { tile_id: String },
}

/// Result of a cascade propagation.
#[derive(Debug, Clone)]
pub struct CascadeResult {
    pub events: Vec<CascadeEvent>,
    pub affected_count: usize,
    pub max_depth_reached: usize,
}

/// Dependency cascade engine.
pub struct TileCascade {
    graph: HashMap<String, Vec<String>>,       // tile -> downstream dependents
    reverse: HashMap<String, Vec<String>>,      // tile -> upstream dependencies
    invalidated: HashSet<String>,
    auto_invalidate: bool,
}

impl TileCascade {
    pub fn new() -> Self {
        Self {
            graph: HashMap::new(),
            reverse: HashMap::new(),
            invalidated: HashSet::new(),
            auto_invalidate: true,
        }
    }

    /// Add a dependency edge: downstream depends on upstream.
    pub fn add_dependency(&mut self, upstream: &str, downstream: &str) {
        self.graph.entry(upstream.to_string()).or_default().push(downstream.to_string());
        self.reverse.entry(downstream.to_string()).or_default().push(upstream.to_string());
    }

    /// Remove a dependency edge.
    pub fn remove_dependency(&mut self, upstream: &str, downstream: &str) {
        if let Some(deps) = self.graph.get_mut(upstream) {
            deps.retain(|d| d != downstream);
        }
        if let Some(deps) = self.reverse.get_mut(downstream) {
            deps.retain(|d| d != upstream);
        }
    }

    /// Propagate an update downstream through all dependents.
    pub fn update_tile(&mut self, tile_id: &str) -> CascadeResult {
        let mut events = vec![CascadeEvent::Updated { tile_id: tile_id.to_string() }];
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        
        if let Some(deps) = self.graph.get(tile_id) {
            for dep in deps {
                queue.push_back((dep.clone(), 1));
            }
        }

        let mut max_depth = 0;
        while let Some((current, depth)) = queue.pop_front() {
            if visited.contains(&current) { continue; }
            visited.insert(current.clone());
            max_depth = max_depth.max(depth);

            if self.auto_invalidate {
                events.push(CascadeEvent::Invalidated {
                    tile_id: current.clone(),
                    reason: format!("upstream {} updated", tile_id),
                });
                self.invalidated.insert(current.clone());
            } else {
                events.push(CascadeEvent::Updated { tile_id: current.clone() });
            }

            if let Some(deps) = self.graph.get(&current) {
                for dep in deps {
                    if !visited.contains(dep) {
                        queue.push_back((dep.clone(), depth + 1));
                    }
                }
            }
        }

        CascadeResult { affected_count: visited.len(), max_depth_reached: max_depth, events }
    }

    /// Invalidate a tile and cascade downstream.
    pub fn invalidate_tile(&mut self, tile_id: &str) -> CascadeResult {
        self.invalidated.insert(tile_id.to_string());
        self._cascade_invalidation(tile_id, "manually invalidated")
    }

    /// Revalidate a tile.
    pub fn revalidate(&mut self, tile_id: &str) {
        self.invalidated.remove(tile_id);
    }

    /// Get all downstream dependents (BFS).
    pub fn downstream(&self, tile_id: &str) -> Vec<String> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut result = Vec::new();
        
        if let Some(deps) = self.graph.get(tile_id) {
            for dep in deps {
                queue.push_back(dep.clone());
            }
        }
        
        while let Some(current) = queue.pop_front() {
            if visited.contains(&current) { continue; }
            visited.insert(current.clone());
            result.push(current.clone());
            if let Some(deps) = self.graph.get(&current) {
                for dep in deps {
                    queue.push_back(dep.clone());
                }
            }
        }
        result
    }

    /// Get all upstream dependencies.
    pub fn upstream(&self, tile_id: &str) -> Vec<String> {
        self.reverse.get(tile_id).cloned().unwrap_or_default()
    }

    /// Check for cycles using DFS. Returns true if a cycle exists.
    pub fn has_cycle(&self) -> bool {
        let mut white = HashSet::new();
        let mut gray = HashSet::new();
        
        for node in self.graph.keys() {
            white.insert(node.clone());
        }
        
        for node in self.graph.keys() {
            if white.contains(node) {
                if self._dfs_cycle(node, &mut white, &mut gray, &mut HashSet::new()) {
                    return true;
                }
            }
        }
        false
    }

    /// Topological sort using Kahn's algorithm.
    pub fn topological_sort(&self) -> Option<Vec<String>> {
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut all_nodes: HashSet<&str> = HashSet::new();
        
        for (node, deps) in &self.graph {
            all_nodes.insert(node);
            for dep in deps {
                all_nodes.insert(dep);
                *in_degree.entry(dep).or_insert(0) += 1;
            }
            in_degree.entry(node).or_insert(0);
        }
        
        let mut queue: VecDeque<&str> = in_degree.iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&n, _)| n)
            .collect();
        
        let mut result = Vec::new();
        while let Some(node) = queue.pop_front() {
            result.push(node.to_string());
            if let Some(deps) = self.graph.get(node) {
                for dep in deps {
                    if let Some(deg) = in_degree.get_mut(dep.as_str()) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(dep);
                        }
                    }
                }
            }
        }
        
        if result.len() == all_nodes.len() { Some(result) } else { None }
    }

    /// Get all invalidated tiles.
    pub fn invalidated_tiles(&self) -> Vec<String> {
        self.invalidated.iter().cloned().collect()
    }

    /// Clear all invalidation marks.
    pub fn clear_invalidations(&mut self) {
        self.invalidated.clear();
    }

    /// Auto-invalidate toggle.
    pub fn set_auto_invalidate(&mut self, enabled: bool) {
        self.auto_invalidate = enabled;
    }

    fn _cascade_invalidation(&mut self, tile_id: &str, reason: &str) -> CascadeResult {
        let mut events = vec![CascadeEvent::Invalidated { tile_id: tile_id.to_string(), reason: reason.to_string() }];
        let deps = self.downstream(tile_id);
        for dep in &deps {
            self.invalidated.insert(dep.clone());
            events.push(CascadeEvent::Invalidated { tile_id: dep.clone(), reason: format!("cascade from {}", tile_id) });
        }
        CascadeResult { affected_count: deps.len() + 1, max_depth_reached: if deps.is_empty() { 0 } else { 1 }, events }
    }

    fn _dfs_cycle(&self, node: &str, white: &mut HashSet<String>, gray: &mut HashSet<String>, _black: &mut HashSet<String>) -> bool {
        white.remove(node);
        gray.insert(node.to_string());
        
        if let Some(deps) = self.graph.get(node) {
            for dep in deps {
                if gray.contains(dep) { return true; }
                if white.contains(dep) && self._dfs_cycle(dep, white, gray, _black) { return true; }
            }
        }
        
        gray.remove(node);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_dependency() {
        let mut c = TileCascade::new();
        c.add_dependency("a", "b");
        c.add_dependency("a", "c");
        assert_eq!(c.downstream("a").len(), 2);
    }

    #[test]
    fn test_update_cascade() {
        let mut c = TileCascade::new();
        c.add_dependency("a", "b");
        c.add_dependency("b", "c");
        let result = c.update_tile("a");
        assert_eq!(result.affected_count, 2);
        assert!(result.events.iter().any(|e| matches!(e, CascadeEvent::Invalidated { tile_id, .. } if tile_id == "b")));
    }

    #[test]
    fn test_invalidate_cascade() {
        let mut c = TileCascade::new();
        c.add_dependency("a", "b");
        c.add_dependency("b", "c");
        c.invalidate_tile("a");
        let inv = c.invalidated_tiles();
        assert!(inv.contains(&"a".to_string()));
        assert!(inv.contains(&"b".to_string()));
        assert!(inv.contains(&"c".to_string()));
    }

    #[test]
    fn test_revalidate() {
        let mut c = TileCascade::new();
        c.add_dependency("a", "b");
        c.invalidate_tile("a");
        c.revalidate("a");
        assert!(!c.invalidated_tiles().contains(&"a".to_string()));
    }

    #[test]
    fn test_downstream() {
        let mut c = TileCascade::new();
        c.add_dependency("a", "b");
        c.add_dependency("b", "c");
        c.add_dependency("a", "d");
        let down = c.downstream("a");
        assert_eq!(down.len(), 3);
    }

    #[test]
    fn test_upstream() {
        let mut c = TileCascade::new();
        c.add_dependency("a", "b");
        c.add_dependency("c", "b");
        let up = c.upstream("b");
        assert_eq!(up.len(), 2);
    }

    #[test]
    fn test_cycle_detection() {
        let mut c = TileCascade::new();
        c.add_dependency("a", "b");
        c.add_dependency("b", "c");
        c.add_dependency("c", "a");
        assert!(c.has_cycle());
    }

    #[test]
    fn test_no_cycle() {
        let mut c = TileCascade::new();
        c.add_dependency("a", "b");
        c.add_dependency("b", "c");
        assert!(!c.has_cycle());
    }

    #[test]
    fn test_topological_sort() {
        let mut c = TileCascade::new();
        c.add_dependency("a", "b");
        c.add_dependency("a", "c");
        c.add_dependency("b", "d");
        let sorted = c.topological_sort().unwrap();
        let a_pos = sorted.iter().position(|x| x == "a").unwrap();
        let b_pos = sorted.iter().position(|x| x == "b").unwrap();
        let d_pos = sorted.iter().position(|x| x == "d").unwrap();
        assert!(a_pos < b_pos);
        assert!(b_pos < d_pos);
    }

    #[test]
    fn test_topological_sort_cycle_returns_none() {
        let mut c = TileCascade::new();
        c.add_dependency("a", "b");
        c.add_dependency("b", "a");
        assert!(c.topological_sort().is_none());
    }

    #[test]
    fn test_remove_dependency() {
        let mut c = TileCascade::new();
        c.add_dependency("a", "b");
        c.remove_dependency("a", "b");
        assert_eq!(c.downstream("a").len(), 0);
    }

    #[test]
    fn test_auto_invalidate_toggle() {
        let mut c = TileCascade::new();
        c.add_dependency("a", "b");
        c.set_auto_invalidate(false);
        let result = c.update_tile("a");
        assert!(!result.events.iter().any(|e| matches!(e, CascadeEvent::Invalidated { .. })));
    }

    #[test]
    fn test_clear_invalidations() {
        let mut c = TileCascade::new();
        c.add_dependency("a", "b");
        c.invalidate_tile("a");
        c.clear_invalidations();
        assert!(c.invalidated_tiles().is_empty());
    }
}
