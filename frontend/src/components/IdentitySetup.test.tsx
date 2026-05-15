import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { IdentitySetup } from "./IdentitySetup";
import { getIdentity } from "../api/client";

// IdentitySetup tests: the first-launch identity entry form.
// Verifies the form requires user + role, calls onSet with the typed
// values, and persists identity to localStorage so the next visit
// auto-loads.

describe("IdentitySetup", () => {
  it("renders all three inputs and a Continue button", () => {
    render(<IdentitySetup onSet={vi.fn()} />);
    expect(screen.getByLabelText(/user id/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/role/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/attributes/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /continue/i })).toBeInTheDocument();
  });

  it("disables Continue until user AND role are entered", async () => {
    const user = userEvent.setup();
    render(<IdentitySetup onSet={vi.fn()} />);
    const btn = screen.getByRole("button", { name: /continue/i });
    expect(btn).toBeDisabled();

    await user.type(screen.getByLabelText(/user id/i), "adam");
    expect(btn).toBeDisabled();  // still missing role

    await user.type(screen.getByLabelText(/^role/i), "analyst_west");
    expect(btn).toBeEnabled();
  });

  it("persists to localStorage AND calls onSet with the typed identity", async () => {
    const user = userEvent.setup();
    const onSet = vi.fn();
    render(<IdentitySetup onSet={onSet} />);

    await user.type(screen.getByLabelText(/user id/i), "adam");
    await user.type(screen.getByLabelText(/^role/i), "analyst_west");
    await user.type(screen.getByLabelText(/attributes/i), "region=west,team=growth");
    await user.click(screen.getByRole("button", { name: /continue/i }));

    expect(onSet).toHaveBeenCalledOnce();
    expect(onSet).toHaveBeenCalledWith({
      user: "adam",
      role: "analyst_west",
      attrs: "region=west,team=growth",
    });
    expect(getIdentity()).toEqual({
      user: "adam",
      role: "analyst_west",
      attrs: "region=west,team=growth",
    });
  });

  it("trims whitespace from inputs before storing", async () => {
    const user = userEvent.setup();
    const onSet = vi.fn();
    render(<IdentitySetup onSet={onSet} />);
    await user.type(screen.getByLabelText(/user id/i), "  adam  ");
    await user.type(screen.getByLabelText(/^role/i), "  analyst  ");
    await user.click(screen.getByRole("button", { name: /continue/i }));
    expect(onSet).toHaveBeenCalledWith({
      user: "adam",
      role: "analyst",
      attrs: "",
    });
  });
});
