import { useScrollCraft } from '../lib/scrollcraft/useScrollCraft';
import { useKvConsole } from '../hooks/useKvConsole';
import { KVHeader } from '../components/kv/KVHeader';
import { KVRequestWorkspace } from '../components/kv/KVRequestWorkspace';
import { KVResponsePanel } from '../components/kv/KVResponsePanel';
import { KVValueInspector } from '../components/kv/KVValueInspector';
import { KVOperationTable } from '../components/kv/KVOperationTable';
import { KVRequestContext } from '../components/kv/KVRequestContext';
import { KVSystemSummary } from '../components/kv/KVSystemSummary';
import './KVConsole.css';

const CLIENT_ID = 'dashboard-client';

export default function KVConsole() {
  const scopeRef = useScrollCraft<HTMLDivElement>();
  const kv = useKvConsole();

  return (
    <div ref={scopeRef}>
      <KVHeader />

      <div className="kv-console__split">
        <KVRequestWorkspace
          request={kv.request}
          phase={kv.phase}
          validationError={kv.validationError}
          requestId={kv.response.requestId}
          clientId={CLIENT_ID}
          onOperationChange={kv.onOperationChange}
          onTargetChange={kv.onTargetChange}
          onConsistencyChange={kv.onConsistencyChange}
          onKeyChange={kv.onKeyChange}
          onValueChange={kv.onValueChange}
          onExampleKey={kv.onExampleKey}
          onExecute={kv.onExecute}
        />
        <KVResponsePanel response={kv.response} phase={kv.phase} />
      </div>

      <KVValueInspector entryKey={kv.viewedKey} entry={kv.entry} />
      <KVOperationTable operations={kv.operations} />
      <KVRequestContext />
      <KVSystemSummary readModel={kv.request.consistency} />
    </div>
  );
}
