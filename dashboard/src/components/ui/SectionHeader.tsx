import type { ReactNode } from 'react';
import './SectionHeader.css';

interface SectionHeaderProps {
  eyebrow?: string;
  title: string;
  description?: string;
  trailing?: ReactNode;
}

export function SectionHeader({ eyebrow, title, description, trailing }: SectionHeaderProps) {
  return (
    <div className="section-header" data-sc-in>
      <div className="section-header__text">
        {eyebrow ? <div className="section-header__eyebrow mono">{eyebrow}</div> : null}
        <h2 className="section-header__title">{title}</h2>
        {description ? <p className="section-header__description">{description}</p> : null}
      </div>
      {trailing ? <div className="section-header__trailing">{trailing}</div> : null}
    </div>
  );
}
