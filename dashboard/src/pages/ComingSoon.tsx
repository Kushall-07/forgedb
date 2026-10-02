import type { LucideIcon } from 'lucide-react';
import './ComingSoon.css';

interface ComingSoonProps {
  icon: LucideIcon;
  title: string;
  description: string;
}

/** Shared placeholder for modules not yet implemented, styled with the same tokens as Overview. */
export function ComingSoon({ icon: Icon, title, description }: ComingSoonProps) {
  return (
    <div className="coming-soon">
      <div className="coming-soon__icon" aria-hidden="true">
        <Icon size={28} strokeWidth={1.75} />
      </div>
      <span className="coming-soon__eyebrow mono">Module coming next</span>
      <h1 className="coming-soon__title">{title}</h1>
      <p className="coming-soon__description">{description}</p>
    </div>
  );
}
