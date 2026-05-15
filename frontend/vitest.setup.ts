// Vitest setup file. Runs once before each test file.
// Adds jest-dom matchers (`toBeInTheDocument`, `toHaveTextContent`,
// `toBeDisabled`, etc.) so component assertions read naturally.

import "@testing-library/jest-dom/vitest";

// Provide a fresh localStorage per test so identity state doesn't
// leak across tests. happy-dom gives us a real Storage implementation
// per test environment, but we wipe it at the start of each test for
// extra safety.
import { beforeEach } from "vitest";

beforeEach(() => {
  localStorage.clear();
});
