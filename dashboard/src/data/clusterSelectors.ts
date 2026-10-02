import { nodes } from './mockCluster';
import type { NodeState } from '../types/cluster';

/**
 * Derived reads over the cluster mock state. Centralized so every component
 * that needs "the leader" or "a node's next index" computes it the same way,
 * and so the future real API only has to replace mockCluster, not every
 * place that reaches into it.
 */
export function getLeader(): NodeState | undefined {
  return nodes.find((n) => n.role === 'leader');
}

export function getFollowers(): NodeState[] {
  return nodes.filter((n) => n.role === 'follower');
}

export function getNode(id: string): NodeState | undefined {
  return nodes.find((n) => n.id === id);
}

/** Raft's nextIndex is always one past what the leader knows a peer has. */
export function nextIndexFor(node: NodeState): number {
  return node.lastLogIndex + 1;
}

/** Leader counts as caught up with itself; followers count once their lag reaches zero. */
export function fullySyncedCount(): number {
  return 1 + getFollowers().filter((n) => n.lag === 0).length;
}

export function isClusterConverged(): boolean {
  const [first, ...rest] = nodes;
  if (!first) return false;
  return rest.every(
    (n) =>
      n.commitIndex === first.commitIndex &&
      n.lastApplied === first.lastApplied &&
      n.lastLogIndex === first.lastLogIndex,
  );
}
