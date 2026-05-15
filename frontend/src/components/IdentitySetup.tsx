import { useState } from "react";
import { setIdentity, type Identity } from "../api/client";

interface Props {
  onSet: (ident: Identity) => void;
}

// First-launch identity setup. In dev mode, gibran's headers ARE the
// identity (no verification), so this is essentially a "who am I
// pretending to be" form. For a JWT-mode deployment, this would be
// replaced by an OIDC login redirect.

export function IdentitySetup({ onSet }: Props) {
  const [user, setUser] = useState("");
  const [role, setRole] = useState("");
  const [attrs, setAttrs] = useState("");

  function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!user.trim() || !role.trim()) return;
    const ident: Identity = {
      user: user.trim(),
      role: role.trim(),
      attrs: attrs.trim(),
    };
    setIdentity(ident);
    onSet(ident);
  }

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        height: "100vh",
      }}
    >
      <div className="card" style={{ width: 420 }}>
        <h2>Welcome to Gibran</h2>
        <p style={{ color: "var(--fg-muted)", marginTop: 0 }}>
          Dev mode -- enter the identity you want to query as. This is
          stored locally and sent with each request as{" "}
          <code>X-Gibran-*</code> headers.
        </p>
        <form onSubmit={submit}>
          <div style={{ marginBottom: 12 }}>
            <label>
              <div style={{ marginBottom: 4 }}>User ID</div>
              <input
                type="text"
                value={user}
                onChange={(e) => setUser(e.target.value)}
                placeholder="adam"
                autoFocus
              />
            </label>
          </div>
          <div style={{ marginBottom: 12 }}>
            <label>
              <div style={{ marginBottom: 4 }}>Role</div>
              <input
                type="text"
                value={role}
                onChange={(e) => setRole(e.target.value)}
                placeholder="analyst_west"
              />
            </label>
          </div>
          <div style={{ marginBottom: 16 }}>
            <label>
              <div style={{ marginBottom: 4 }}>
                Attributes <span style={{ color: "var(--fg-muted)" }}>(optional)</span>
              </div>
              <input
                type="text"
                value={attrs}
                onChange={(e) => setAttrs(e.target.value)}
                placeholder="region=west,team=growth"
              />
            </label>
          </div>
          <button type="submit" className="primary" disabled={!user.trim() || !role.trim()}>
            Continue
          </button>
        </form>
      </div>
    </div>
  );
}
