import { Route, Routes } from 'react-router-dom';
import { AppShell } from '../components/layout/AppShell';
import Overview from '../pages/Overview';
import Cluster from '../pages/Cluster';
import KVConsole from '../pages/KVConsole';
import Raft from '../pages/Raft';
import Observability from '../pages/Observability';

export function AppRoutes() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route path="/" element={<Overview />} />
        <Route path="/cluster" element={<Cluster />} />
        <Route path="/kv" element={<KVConsole />} />
        <Route path="/raft" element={<Raft />} />
        <Route path="/observability" element={<Observability />} />
      </Route>
    </Routes>
  );
}
