{
  description = "Local-first Notion knowledge MCP";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs = { nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
    in {
      devShells = nixpkgs.lib.genAttrs systems (system:
        let
          pkgs = import nixpkgs { inherit system; };
        in {
          default = pkgs.mkShell {
            packages = with pkgs; [
              cargo
              cargo-audit
              gitleaks
              rustc
              rustfmt
              clippy
              git
              pkg-config
              python3
              openssl
              sqlite
            ];

            RUST_BACKTRACE = "1";
          };
        });
    };
}

# CI selection benchmark: toolchain
