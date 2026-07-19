{
  description = "A Nix flake based Python environment";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

    # Python inputs
    pyproject-nix = {
      url = "github:nix-community/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    # Dev inputs
    git-hooks = {
      url = "github:cachix/git-hooks.nix/master";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      pyproject-nix,
      uv2nix,
      pyproject-build-systems,
      git-hooks,
    }@inputs:
    let
      inherit (nixpkgs) lib;

      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forEachSupportedSystem =
        f: lib.genAttrs supportedSystems (system: f { pkgs = import nixpkgs { inherit system; }; });

      workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };
      overlay = workspace.mkPyprojectOverlay { sourcePreference = "wheel"; };
      pyprojectOverrides = _final: _prev: {
        # Implement build fixups here.
        # Note that uv2nix is _not_ using Nixpkgs buildPythonPackage.
        # It's using https://pyproject-nix.github.io/pyproject.nix/build.html
      };
    in
    {
      checks = forEachSupportedSystem (
        { pkgs }:
        {
          pre-commit-check = inputs.git-hooks.lib.${pkgs.stdenv.hostPlatform.system}.run {
            src = builtins.path {
              path = ./.;
              name = "dagster-webserver";
            };
            package = pkgs.prek;
            hooks = {
              trufflehog = {
                enable = true;
                name = "🔒 Security · Detect hardcoded secrets";
              };
              nixfmt-rfc-style = {
                enable = true;
                name = "🔍 Code Quality · ❄️ Nix · Format";
                after = [ "trufflehog" ];
              };
              ruff = {
                enable = true;
                name = "🔍 Code Quality · 🐍 Python · Fix";
                args = [
                  "--extend-select"
                  "I"
                ];
                after = [ "trufflehog" ];
              };
              ruff-format = {
                enable = true;
                name = "🔍 Code Quality · 🐍 Python · Format";
                after = [ "ruff" ];
              };
              flake-checker = {
                enable = true;
                name = "✅ Data & Config Validation · ❄️ Nix · Flake checker";
                args = [
                  "--check-supported"
                  "false"
                ];
                after = [
                  "nixfmt-rfc-style"
                  "ruff"
                  "ruff-format"
                ];
              };
              check-yaml = {
                enable = true;
                name = "✅ Data & Config Validation · YAML · Lint";
                after = [
                  "nixfmt-rfc-style"
                  "ruff"
                  "ruff-format"
                ];
              };
              mdformat = {
                enable = true;
                name = "📝 Docs · Markdown · Format";
                after = [
                  "flake-checker"
                  "check-yaml"
                ];
              };
              just =
                let
                  package = pkgs.just;
                in
                {
                  enable = true;
                  package = package;
                  name = "🤖 Justfile · Format";
                  entry = "${package}/bin/just --fmt --unstable";
                  files = "^justfile$";
                  pass_filenames = false;
                  after = [ "mdformat" ];
                };
              check-case-conflicts = {
                enable = true;
                name = "📁 Filesystem · Check case sensitivity";
                after = [ "just" ];
              };
              check-symlinks = {
                enable = true;
                name = "📁 Filesystem · Check symlinks";
                after = [ "just" ];
              };
              check-merge-conflicts = {
                enable = true;
                name = "🌳 Git Quality · Detect conflict markers";
                after = [
                  "check-symlinks"
                  "check-case-conflicts"
                ];
              };
              forbid-new-submodules = {
                enable = true;
                name = "🌳 Git Quality · Prevent submodule creation";
                after = [
                  "check-symlinks"
                  "check-case-conflicts"
                ];
              };
              no-commit-to-branch = {
                enable = true;
                name = "🌳 Git Quality · Protect main branch";
                settings.branch = [ "main" ];
                stages = [ "pre-push" ];
                after = [
                  "check-symlinks"
                  "check-case-conflicts"
                ];
              };
              check-added-large-files = {
                enable = true;
                name = "🌳 Git Quality · Block large file commits";
                args = [ "--maxkb=5000" ];
                after = [
                  "check-symlinks"
                  "check-case-conflicts"
                ];
              };
              commitizen = {
                enable = true;
                name = "🌳 Git Quality · Validate commit message";
                stages = [ "commit-msg" ];
                after = [
                  "check-symlinks"
                  "check-case-conflicts"
                ];
              };
            };
          };
        }
      );

      packages = forEachSupportedSystem (
        { pkgs }:
        let
          dagster-js-modules = pkgs.stdenv.mkDerivation rec {
            name = "dagster-js-modules";
            version = "1.13.9";
            src = pkgs.fetchFromGitHub {
              owner = "dagster-io";
              repo = "dagster";
              rev = "${version}";
              hash = "sha256-kIqq5rUoo89yBC2hyAsHZHDxOERSYviODEEpuArsYlY=";
            };
            patches = [
              ./patches/add-logout-button.patch
              ./patches/admin-portal-ui.patch
            ];
            buildInputs = (
              builtins.attrValues {
                inherit (pkgs)
                  cacert
                  corepack
                  nodejs
                  uv
                  ;
              }
            );
            buildPhase = ''
              runHook preBuild
              pushd js_modules

              mkdir -p .tmp
              mkdir -p .home
              mkdir -p .corepack

              export TMPDIR="$(pwd)/.tmp"
              export HOME=$(pwd)/.home

              corepack enable --install-directory ./.corepack
              ./.corepack/yarn install || ./.corepack/yarn install
              ./.corepack/yarn workspace @dagster-io/app-oss build
              popd
              runHook postBuild
            '';
            installPhase = ''
              runHook preInstall
              mkdir -p $out/lib
              cp -R ./python_modules/dagster-webserver/dagster_webserver/webapp/build "$out/lib/dagster-app-oss"
              runHook postInstall
            '';
          };
          pythonSet =
            # Use base package set from pyproject.nix builders
            (pkgs.callPackage pyproject-nix.build.packages {
              python = pkgs.python3;
            }).overrideScope
              (
                lib.composeManyExtensions [
                  pyproject-build-systems.overlays.default
                  overlay
                  pyprojectOverrides
                  (final: prev: {
                    dagster-webserver = prev.dagster-webserver.overrideAttrs (old: {
                      nativeBuildInputs = [ dagster-js-modules ] ++ old.nativeBuildInputs;
                      patchPhase = ''
                        runHook prePatch
                        mkdir -p ./dagster_webserver/webapp
                        cp -R "${dagster-js-modules}/lib/dagster-app-oss"  ./dagster_webserver/webapp/build
                        runHook postPatch
                      '';
                    });
                  })
                ]
              );
        in
        {
          default = pythonSet.mkVirtualEnv "dagster-webserver-env" workspace.deps.default;
          dagster-js-modules = dagster-js-modules;
        }
      );

      apps = forEachSupportedSystem (
        { pkgs }:
        {
          default = {
            type = "app";
            program = "${self.packages.${pkgs.stdenv.hostPlatform.system}.default}/bin/dagster-webserver";
          };
        }
      );

      devShells = forEachSupportedSystem (
        { pkgs }:
        let
          inherit (self.checks.${pkgs.stdenv.hostPlatform.system}.pre-commit-check) shellHook enabledPackages;
          pythonSet =
            # Use base package set from pyproject.nix builders
            (pkgs.callPackage pyproject-nix.build.packages {
              python = pkgs.python3;
            }).overrideScope
              (
                lib.composeManyExtensions [
                  pyproject-build-systems.overlays.default
                  overlay
                  pyprojectOverrides
                ]
              );
        in
        {
          default =
            let
              # Create an overlay enabling editable mode for all local dependencies.
              editableOverlay = workspace.mkEditablePyprojectOverlay {
                # Use environment variable
                root = "$REPO_ROOT";
                # Optional: Only enable editable for these packages
                # members = [ "dagster-webserver" ];
              };
              # Override previous set with our overrideable overlay.
              editablePythonSet = pythonSet.overrideScope (
                lib.composeManyExtensions [
                  editableOverlay

                  # Apply fixups for building an editable package of your workspace packages
                  (final: prev: {
                    dagster-webserver = prev.dagster-webserver.overrideAttrs (old: {
                      # It's a good idea to filter the sources going into an editable build
                      # so the editable package doesn't have to be rebuilt on every change.
                      src = lib.fileset.toSource {
                        root = old.src;
                        fileset = lib.fileset.unions [
                          (old.src + "/pyproject.toml")
                          (old.src + "/README.md")
                          (old.src + "/dagster_webserver")
                        ];
                      };

                      # Hatchling (our build system) has a dependency on the `editables` package when building editables.
                      #
                      # In normal Python flows this dependency is dynamically handled, and doesn't need to be explicitly declared.
                      # This behaviour is documented in PEP-660.
                      #
                      # With Nix the dependency needs to be explicitly declared.
                      nativeBuildInputs =
                        old.nativeBuildInputs
                        ++ final.resolveBuildSystem {
                          editables = [ ];
                        };
                    });

                  })
                ]
              );

              # Build virtual environment, with local packages being editable.
              #
              # Enable all optional dependencies for development.
              virtualenv = editablePythonSet.mkVirtualEnv "dagster-webserver-dev-env" workspace.deps.all;

            in
            pkgs.mkShell {
              buildInputs = (builtins.attrValues { inherit (pkgs) prek; }) ++ enabledPackages;
              packages = [
                virtualenv
                pkgs.uv
                pkgs.just
                pkgs.nil
                pkgs.nixfmt-rfc-style
              ];

              env = {
                # Don't create venv using uv
                UV_NO_SYNC = "1";

                # Force uv to use Python interpreter from venv
                UV_PYTHON = "${virtualenv}/bin/python";

                # Prevent uv from downloading managed Python's
                UV_PYTHON_DOWNLOADS = "never";

                shell = "zsh";
                NIL_PATH = "${pkgs.nil}/bin/nil";
              };

              shellHook = lib.strings.concatLines [
                shellHook
                # Undo dependency propagation by nixpkgs.
                "unset PYTHONPATH"
                # Get repository root using git. This is expanded at runtime by the editable `.pth` machinery.
                "export REPO_ROOT=$(${pkgs.git}/bin/git rev-parse --show-toplevel 2>/dev/null || true)"
              ];
            };
        }
      );
    };
}
