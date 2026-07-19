# UI Integration — Admin Portal Button

## Overview

This document details how the admin portal is integrated into the Dagster UI. The key integration points are:

1. **Navigation button** — an "Admin" link in the bottom nav (only visible to users with any admin permission)
1. **`/api/me` response** — extended to include `hasAnyAdminPermission` flag
1. **Conditional rendering** — the nav item returns `null` for users with no admin permissions

## Reference: Logout Button Patch

The `patches/add-logout-button.patch` shows the established pattern for adding nav items:

```tsx
// Pattern: new component + add to getBottomGroups()

const LogoutItem = () => {
  const {isCollapsed} = useContext(NavCollapseContext);

  const handleLogout = () => {
    fetch('/logout', {method: 'POST'})
      .catch(() => {})
      .finally(() => {
        window.location.href = '/login';
      });
  };

  return (
    <Tooltip content="Sign out" placement="right" canShow={isCollapsed}>
      <UnstyledButton onClick={handleLogout} className={styles.itemButton}>
        <NavItemContent
          icon={<Icon name="logout" />}
          label="Sign out"
          collapsed={isCollapsed}
        />
      </UnstyledButton>
    </Tooltip>
  );
};

// Added to getBottomGroups():
{
  key: 'logout',
  label: 'Sign out',
  element: <LogoutItem />,
},
```

## Admin Portal Button Component

We follow the same pattern but with **conditional rendering** based on user permissions:

```tsx
// js_modules/ui-core/src/app/navigation/AdminPortalItem.tsx

import { Icon, Tooltip, UnstyledButton } from "@dagster-io/ui-components";
import { useContext } from "react";
import { useCurrentUser } from "@shared/app/useCurrentUser";
import { NavCollapseContext } from "./NavCollapseProvider";
import { NavItemContent } from "./NavItemContent";
import styles from "./css/MainNavigation.module.css";

const AdminPortalItem = () => {
  const { isCollapsed } = useContext(NavCollapseContext);
  const { data: user } = useCurrentUser();

  // Only render if user has ANY admin permission
  if (!user?.hasAnyAdminPermission) {
    return null;
  }

  return (
    <Tooltip content="Admin Portal" placement="right" canShow={isCollapsed}>
      <UnstyledButton
        onClick={() => (window.location.href = "/admin")}
        className={styles.itemButton}
      >
        <NavItemContent
          icon={<Icon name="settings_admin" />}
          label="Admin"
          collapsed={isCollapsed}
        />
      </UnstyledButton>
    </Tooltip>
  );
};

export default AdminPortalItem;
```

### Why `UnstyledButton` + `onClick` Instead of `NavItemWithLink`

The logout button uses `UnstyledButton` with `onClick` because it performs an action (POST to `/logout`). The admin portal button is a simple navigation link, so we could use either approach:

- **`NavItemWithLink`** — uses `<a href="/admin">` with `isActive` check for highlighting
- **`UnstyledButton` + `onClick`** — uses `window.location.href` for navigation

We use `UnstyledButton` to match the logout pattern, but `NavItemWithLink` is also valid and provides active-state highlighting.

## Navigation Placement

The admin portal button goes in the **bottom navigation group**, between Collapse and Settings:

```tsx
// js_modules/ui-core/src/app/navigation/mainNavigationItems.tsx

import AdminPortalItem from "./AdminPortalItem";

export const getBottomGroups = (
  _config: NavigationGroupConfig,
): NavigationGroup[] => {
  const searchGroup = [
    {
      key: "search",
      items: [{ key: "search", label: "Search", element: <SearchItem /> }],
    },
  ];

  const adminGroup = {
    key: "support",
    items: [
      { key: "collapse", label: "Collapse", element: <CollapseItem /> },
      { key: "admin", label: "Admin", element: <AdminPortalItem /> }, // NEW
      { key: "settings", label: "Settings", element: <SettingsItem /> },
      { key: "support", label: "Support", element: <SupportItem /> },
    ],
  };

  return [...searchGroup, adminGroup];
};
```

The `AdminPortalItem` component returns `null` for non-admin users, so the item simply doesn't appear in the navigation. No special conditional logic needed in `getBottomGroups()`.

## Backend Support — `/api/me` Extension

The existing `me_endpoint` in `dagster_webserver/auth/routes.py` returns basic user info. We extend it to include `hasAnyAdminPermission` — whether the user has any admin portal permission:

```python
# dagster_webserver/auth/routes.py — me_endpoint (extended)

async def me_endpoint(request: Request) -> JSONResponse:
    """Return current user info for the UI."""
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse(
            {"error": "Not authenticated"},
            status_code=HTTP_401_UNAUTHORIZED,
        )

    # Check if user has ANY admin portal permission
    auth_provider = getattr(request.app.state, "auth_provider", None)
    has_any_admin = False
    if auth_provider:
        from dagster_webserver.admin.permissions import has_any_admin_permission
        perms = auth_provider.get_user_permissions(user)
        has_any_admin = has_any_admin_permission(perms)

    return JSONResponse({
        "username": user.username,
        "role": user.role,
        "email": user.email,
        "displayName": user.display_name,
        "hasAnyAdminPermission": has_any_admin,  # NEW
    })
```

## Frontend Hook — `useCurrentUser`

The `useCurrentUser` hook (or equivalent) calls `/api/me` and caches the response. The UI components use this to conditionally render admin-only features:

```tsx
// js_modules/ui-core/src/app/useCurrentUser.tsx (conceptual)

interface CurrentUser {
  username: string;
  role: string;
  email: string | null;
  displayName: string | null;
  hasAnyAdminPermission: boolean; // NEW
}

export function useCurrentUser() {
  return useQuery<CurrentUser>({
    url: "/api/me",
    staleTime: 5 * 60 * 1000, // Cache for 5 minutes
  });
}
```

## Conditional Rendering Pattern

The admin portal button follows the same conditional rendering pattern used elsewhere in the Dagster UI:

```tsx
// Pattern: check data, return null if not authorized

const AdminPortalItem = () => {
  const { data: user } = useCurrentUser();

  // Early return — item doesn't exist in the DOM
  if (!user?.hasAnyAdminPermission) {
    return null;
  }

  // Render the nav item
  return <NavItemContent icon={<Icon name="settings_admin" />} label="Admin" />;
};
```

This is the same pattern used for:

- Feature-flagged nav items (`useVisibleFeatureFlagRows`)
- Job-state-dependent nav items (`jobState === 'has-jobs'`)
- Auth-dependent nav items (logout button only shown when auth is enabled)

## Icon Selection

The admin portal button needs an icon. Options from the Dagster icon set:

| Icon Name | Description | Suitability |
| ---------------------- | -------------------- | ------------------------------ |
| `settings_admin` | Admin settings gear | ★★★★★ Best fit |
| `shield` | Shield/protection | ★★★★ Good for security |
| `cog` | Generic settings | ★★★ Generic |
| `lock` | Lock/security | ★★★ More about auth than admin |
| `admin_panel_settings` | Material admin panel | ★★★★★ Good alternative |

We use `settings_admin` as the primary icon, with `shield` as a fallback if the icon is not available in the current icon set.

## Active State Highlighting

The admin portal button should be highlighted when the user is on an admin page. Using `NavItemWithLink` provides this automatically:

```tsx
// Alternative: use NavItemWithLink for active state

const AdminPortalItem = () => {
  const { data: user } = useCurrentUser();

  if (!user?.hasAnyAdminPermission) {
    return null;
  }

  return (
    <NavItemWithLink
      icon={<Icon name="settings_admin" />}
      label="Admin"
      href="/admin"
      isActive={(_, currentLocation) =>
        currentLocation.pathname.startsWith("/admin")
      }
    />
  );
};
```

The `isActive` function checks if the current path starts with `/admin`, so the button is highlighted for all admin sub-pages (`/admin/users`, `/admin/roles`, etc.).

## Security Considerations

### Client-Side Hiding Is Not Security

The conditional rendering (`return null`) is a **UX concern**, not a security measure. A determined user could still navigate to `/admin` directly. The actual security is enforced server-side:

1. **`AdminPortalMiddleware`** checks if user has ANY admin permission
1. **View `is_accessible()`** checks view-specific permissions (e.g., `ADMIN_VIEW_USERS`)
1. **View `can_create/edit/delete()`** checks operation-specific permissions
1. **Template rendering** hides buttons based on permissions

The client-side check simply improves UX by not showing links to pages the user can't access.

### What Happens If a Non-Admin User Navigates to `/admin`

```
GET /admin
  → AuthMiddleware: user is authenticated ✓
  → AdminPortalMiddleware: checks has_any_admin_permission(perms)
    → No admin permissions → 403 Forbidden
```

The user sees a 403 error page. They are NOT redirected to login (they are already authenticated), but they cannot access the admin portal.

## Testing the Integration

### Unit Tests

```tsx
// AdminPortalItem.test.tsx

describe("AdminPortalItem", () => {
  it("renders when user has admin access", () => {
    mockUseCurrentUser({ data: { hasAnyAdminPermission: true } });
    render(<AdminPortalItem />);
    expect(screen.getByText("Admin")).toBeInTheDocument();
  });

  it("does not render when user lacks admin access", () => {
    mockUseCurrentUser({ data: { hasAnyAdminPermission: false } });
    render(<AdminPortalItem />);
    expect(screen.queryByText("Admin")).not.toBeInTheDocument();
  });

  it("does not render when user is not loaded", () => {
    mockUseCurrentUser({ data: null });
    render(<AdminPortalItem />);
    expect(screen.queryByText("Admin")).not.toBeInTheDocument();
  });
});
```

### Integration Tests

```python
# Test that /api/me includes hasAnyAdminPermission
def test_me_endpoint_admin_access(admin_client):
    response = admin_client.get("/api/me")
    assert response.status_code == 200
    data = response.json()
    assert data["hasAnyAdminPermission"] is True

def test_me_endpoint_no_admin_access(viewer_client):
    response = viewer_client.get("/api/me")
    assert response.status_code == 200
    data = response.json()
    assert data["hasAnyAdminPermission"] is False
```

## Future Extensions

### Admin Badge

When the user is on a non-admin page, show a small badge or indicator that admin features are available:

```tsx
// In the top nav, show a small gear icon with tooltip
if (user?.hasAnyAdminPermission) {
  return (
    <Tooltip content="Admin Portal" placement="bottom">
      <Icon name="settings_admin" size="sm" className="admin-badge" />
    </Tooltip>
  );
}
```

### Admin-Only Feature Flags

Future admin features (API tokens, audit log, SSO settings) can be gated with additional flags:

```tsx
interface CurrentUser {
  // ...
  hasAnyAdminPermission: boolean;
  adminPermissions: {
    viewUsers?: boolean;
    createUser?: boolean;
    editUser?: boolean;
    deleteUser?: boolean;
    viewRoles?: boolean;
    createRole?: boolean;
    editRole?: boolean;
    deleteRole?: boolean;
    manageTokens?: boolean; // Future
    viewAuditLog?: boolean; // Future
  };
}
```

This allows the admin portal to show/hide sections based on the user's specific permissions, not just a binary "admin or not."
