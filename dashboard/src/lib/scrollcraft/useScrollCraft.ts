import { useEffect, useRef } from 'react';
import './scrollcraft.js';
// scrollcraft.d.ts is picked up automatically by tsconfig's `include: ["src"]`.

/**
 * Mounts the scroll-craft engine against a scoped container so it only reads
 * `data-sc-*` attributes within this component's subtree, and tears it down
 * on unmount (route changes in particular, since each page mounts its own
 * scope). See src/lib/scrollcraft/scrollcraft.js for the attribute reference.
 */
export function useScrollCraft<T extends HTMLElement>() {
  const ref = useRef<T | null>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const instance = window.ScrollCraft.mount(el);
    return () => instance.destroy();
  }, []);

  return ref;
}
