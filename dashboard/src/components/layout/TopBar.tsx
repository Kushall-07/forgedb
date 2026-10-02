import { Menu, X } from 'lucide-react';
import { cluster } from '../../data/mockCluster';
import { StatusIndicator } from '../ui/StatusIndicator';
import './TopBar.css';

interface TopBarProps {
  mobileOpen: boolean;
  onToggleMobile: () => void;
}

export function TopBar({ mobileOpen, onToggleMobile }: TopBarProps) {
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
        <StatusIndicator tone="sync" label="Cluster Healthy" pulse size="sm" />
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
