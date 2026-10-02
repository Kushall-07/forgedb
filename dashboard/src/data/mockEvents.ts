import type { RaftEvent } from '../types/cluster';

/** Mock recent Raft events, newest last (rendered oldest-to-newest on a timeline). */
export const events: RaftEvent[] = [
  { id: 'evt-1', timestamp: '12:41:02', message: 'NODE-2 elected leader', kind: 'election' },
  { id: 'evt-2', timestamp: '12:41:03', message: 'AppendEntries accepted by NODE-1', kind: 'append' },
  { id: 'evt-3', timestamp: '12:41:03', message: 'AppendEntries accepted by NODE-3', kind: 'append' },
  { id: 'evt-4', timestamp: '12:41:03', message: 'Log entry committed at index 4', kind: 'commit' },
  { id: 'evt-5', timestamp: '12:41:03', message: 'State machine applied index 4', kind: 'apply' },
  { id: 'evt-6', timestamp: '12:41:04', message: 'Cluster synchronized', kind: 'sync' },
];
