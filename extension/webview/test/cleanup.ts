// Unmount what each test rendered: a mocha root hook, as the tdd interface has no afterEach for
// Testing Library to find. Required after setup.ts, once the DOM exists.

import { cleanup } from '@testing-library/react';

export const mochaHooks = {
  afterEach(): void {
    cleanup();
  },
};
