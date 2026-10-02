import { nodes } from '../../data/mockCluster';
import { isClusterConverged } from '../../data/clusterSelectors';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator } from '../ui/StatusIndicator';
import './RaftStateComparison.css';

const ROWS: Array<{ label: string; read: (n: (typeof nodes)[number]) => string }> = [
  { label: 'Role', read: (n) => n.role.toUpperCase() },
  { label: 'Term', read: (n) => String(n.term) },
  { label: 'Commit', read: (n) => String(n.commitIndex) },
  { label: 'Applied', read: (n) => String(n.lastApplied) },
  { label: 'Log', read: (n) => String(n.lastLogIndex) },
];

/** A side-by-side read of all three nodes, laid out to make convergence (or divergence) visually obvious at a glance. */
export function RaftStateComparison() {
  const converged = isClusterConverged();

  return (
    <section className="raft-comparison" aria-label="Raft state comparison">
      <SectionHeader
        eyebrow="Consensus"
        title="Raft State Comparison"
        description="The same Raft fields read from every node, aligned so identical values are obvious."
      />

      <div className="raft-comparison__table-wrap panel" data-sc-in>
        <table className="raft-comparison__table">
          <thead>
            <tr>
              <th scope="col" />
              {nodes.map((n) => (
                <th scope="col" className="mono" key={n.id}>
                  {n.id}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {ROWS.map((row) => (
              <tr key={row.label}>
                <th scope="row" className="mono">
                  {row.label.toUpperCase()}
                </th>
                {nodes.map((n) => (
                  <td className="mono" key={n.id}>
                    {row.read(n)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {converged ? (
        <p className="raft-comparison__note">
          <StatusIndicator tone="sync" label="Converged" size="sm" /> All nodes currently report the same
          committed/applied boundary.
        </p>
      ) : null}
    </section>
  );
}
