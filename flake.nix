{
  description = "Local-first Notion knowledge MCP";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
  inputs.rust-overlay.url = "github:oxalica/rust-overlay";
  inputs.rust-overlay.inputs.nixpkgs.follows = "nixpkgs";

  outputs = { nixpkgs, rust-overlay, ... }:
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
          # The distro compiler links LLVM 21, whose AVX512 intrinsic ABI
          # mismatches Rust 1.98. Use upstream's matched compiler/LLVM and tools.
          spikePkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };
          spikeToolchain = spikePkgs.rust-bin.stable."1.98.1".minimal.override {
            extensions = [ "clippy" "rustfmt" ];
          };
        in {
          format = pkgs.mkShellNoCC {
            packages = with pkgs; [ cargo rustfmt ];
          };
          security = pkgs.mkShellNoCC {
            packages = with pkgs; [ cargo-audit gitleaks git python3 ];
          };
          spike = pkgs.mkShell {
            packages = with pkgs; [ spikeToolchain pkg-config openssl protobuf python3 curl time ];
            RUST_BACKTRACE = "1";
          };
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
