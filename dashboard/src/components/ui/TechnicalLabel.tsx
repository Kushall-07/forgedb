import type { ReactNode } from 'react';
import './TechnicalLabel.css';

interface TechnicalLabelProps {
  children: ReactNode;
  as?: 'span' | 'div';
}

/** The small uppercase mono eyebrow used for field names throughout the console. */
export function TechnicalLabel({ children, as = 'span' }: TechnicalLabelProps) {
  const Tag = as;
  return <Tag className="technical-label">{children}</Tag>;
}
