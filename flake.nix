{
  description = "A Nix flake for flakenet (go) development environment.";

  inputs = {
    # nixos-unstable gives us the latest packages.
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    # flake-utils helps us write flakes that work on multiple systems (Linux, macOS).
    # Documentation: https://github.com/numtide/flake-utils
    flake-utils.url = "github:numtide/flake-utils";

    # Pinned input for Go, so the toolchain matches go.mod instead of drifting
    # with unstable. Same revision as Diarkis/core.
    nixpkgs-go.url = "github:NixOS/nixpkgs/e607cb5360ff1234862ac9f8839522becb853bb9";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      nixpkgs-go,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        pkgs-go = nixpkgs-go.legacyPackages.${system};
      in
      {
        # Activated with 'nix develop', or automatically via direnv.
        devShells.default = pkgs.mkShell {
          buildInputs = [
            pkgs-go.go_1_25
            pkgs-go.golangci-lint
            pkgs-go.govulncheck
            pkgs-go.delve
            pkgs-go.gopls

            pkgs.git
          ];
        };
      }
    );
}
