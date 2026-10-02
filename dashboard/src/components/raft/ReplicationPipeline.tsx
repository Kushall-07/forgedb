import { getRaftFollowers, getRaftLeader, raftConsensus } from '../../data/raftSelectors';
import { SectionHeader } from '../ui/SectionHeader';
import './ReplicationPipeline.css';

/**
 * The conceptual path every write takes. There is deliberately no direct
 * Leader -> Storage stage: replication happens before commitment, a majority
 * ack is what enables commitment, and application to the state machine
 * follows only after commitment.
 */
export function ReplicationPipeline() {
  const leader = getRaftLeader();
  const followers = getRaftFollowers();
  if (!leader) return null;

  return (
    <section className="replication-pipeline" aria-label="Replication pipeline">
      <SectionHeader
        eyebrow="Consensus"
        title="Replication Pipeline"
        description="Replication happens before commitment. A majority acknowledgment enables commitment. Application to the state machine follows only after commitment — never before."
      />

      <div className="replication-pipeline__flow panel" data-sc-in>
        <div className="replication-pipeline__stage replication-pipeline__stage--leader">
          <span className="replication-pipeline__stage-label mono">LEADER</span>
          <span className="replication-pipeline__stage-value mono">{leader.nodeId}</span>
        </div>

        <div className="replication-pipeline__connector" aria-hidden="true">
          <span className="replication-pipeline__arrow" />
          <span className="replication-pipeline__connector-label mono">AppendEntries</span>
        </div>

        <div className="replication-pipeline__stage replication-pipeline__stage--followers">
          <span className="replication-pipeline__stage-label mono">FOLLOWER REPLICATION</span>
          <div className="replication-pipeline__followers">
            {followers.map((f) => (
              <span className="replication-pipeline__follower mono" key={f.nodeId}>
                {f.nodeId}
              </span>
            ))}
          </div>
        </div>

        <div className="replication-pipeline__connector" aria-hidden="true">
          <span className="replication-pipeline__arrow" />
          <span className="replication-pipeline__connector-label mono">Majority Ack</span>
        </div>

        <div className="replication-pipeline__stage replication-pipeline__stage--majority">
          <span className="replication-pipeline__stage-label mono">MAJORITY ACK</span>
          <span className="replication-pipeline__stage-value mono">
            {raftConsensus.majorityCount} / {raftConsensus.totalNodes}
          </span>
        </div>

        <div className="replication-pipeline__connector" aria-hidden="true">
          <span className="replication-pipeline__arrow" />
        </div>

        <div className="replication-pipeline__stage replication-pipeline__stage--commit">
          <span className="replication-pipeline__stage-label mono">COMMIT INDEX</span>
          <span className="replication-pipeline__stage-value mono">{raftConsensus.commitIndex}</span>
        </div>

        <div className="replication-pipeline__connector" aria-hidden="true">
          <span className="replication-pipeline__arrow" />
          <span className="replication-pipeline__connector-label mono">Apply</span>
        </div>

        <div className="replication-pipeline__stage replication-pipeline__stage--apply">
          <span className="replication-pipeline__stage-label mono">LAST APPLIED</span>
          <span className="replication-pipeline__stage-value mono">{raftConsensus.lastApplied}</span>
        </div>
      </div>

      <p className="replication-pipeline__caption">
        There is no direct Leader &rarr; Storage path in Raft: every write is ordered through the replicated log
        before anything is applied, and followers never apply independently of the commit boundary.
      </p>
    </section>
  );
}
