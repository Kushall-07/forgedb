// Ambient types for the vanilla-JS scroll-craft engine (engine/scrollcraft.js),
// which attaches itself to `window.ScrollCraft` and is never imported as a module.

export interface ScrollCraftInstance {
  layout(): void;
  read(): void;
  destroy(): void;
}

export interface ScrollCraftGlobal {
  mount(root?: Element | Document | string, opts?: { lerp?: number }): ScrollCraftInstance;
  reduce: boolean;
  instances: ScrollCraftInstance[];
}

declare global {
  interface Window {
    ScrollCraft: ScrollCraftGlobal;
  }
}

export {};
