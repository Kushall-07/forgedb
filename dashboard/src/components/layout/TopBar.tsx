import { Menu, X } from 'lucide-react';
import { useBackendCluster } from '../../hooks/useBackendCluster';
import { DataSourceBadge } from '../ui/DataSourceBadge';
import './TopBar.css';

interface TopBarProps {
  mobileOpen: boolean;
  onToggleMobile: () => void;
}

export function TopBar({ mobileOpen, onToggleMobile }: TopBarProps) {
  const { status, cluster } = useBackendCluster();

  return (
    <header className="topbar">
      <div className="topbar__left">
        <button
          type="button"
          className="topbar__menu-btn"
          onClick={onToggleMobile}
          aria-label={mobileOpen ? 'Close navigation' : 'Open navigation'}
          aria-expanded={mobileOpen}
        >
          {mobileOpen ? <X size={18} /> : <Menu size={18} />}
        </button>
        <span className="topbar__title mono">FORGEDB CONTROL PLANE</span>
      </div>

      <div className="topbar__right">
        <DataSourceBadge status={status} />
        <span className="topbar__divider" aria-hidden="true" />
        <span className="topbar__field">
          <span className="topbar__field-label mono">Leader</span>
          <span className="topbar__field-value mono">{cluster.leader}</span>
        </span>
        <span className="topbar__field">
          <span className="topbar__field-label mono">Term</span>
          <span className="topbar__field-value mono">{cluster.term}</span>
        </span>
      </div>
    </header>
  );
}
