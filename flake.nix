{
  description = "ahole — send a file or a directory to a friend, peer to peer";

  inputs = {
    # Pinned to the same channel as this machine's system, so the toolchain and
    # the musl cross compiler come straight out of the local nix store.
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      nixpkgs,
      rust-overlay,
      ...
    }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ (import rust-overlay) ];
      };

      target = "x86_64-unknown-linux-musl";

      # nixpkgs' own rustc ships std for this machine only, and its musl-cross
      # rustc ships std for musl only — neither can build a musl binary on its
      # own, because the proc macros in the dependency tree have to be compiled
      # for the host. rust-overlay gives one toolchain that holds both.
      toolchain = pkgs.rust-bin.stable.latest.default.override {
        targets = [ target ];
        extensions = [
          "rust-src"
          "rust-analyzer"
        ];
      };

      # `ring` compiles C, and linking statically needs musl's libc.a, so the
      # build wants a C toolchain aimed at musl as well as the Rust one.
      muslCc = pkgs.pkgsCross.musl64.stdenv.cc;
      muslPrefix = muslCc.targetPrefix;

      buildStatic = pkgs.writeShellScriptBin "build-static" ''
        set -euo pipefail
        cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)"
        cargo build --release --target ${target} "$@"
        binary="target/${target}/release/ahole"
        echo
        echo "$binary"
        ls -lh "$binary" | awk '{print "  size:  " $5}'
        file "$binary" | sed 's/^[^:]*: /  type:  /'
      '';

      # The package, for installing rather than hacking on. It builds with
      # whatever Rust the nixpkgs it is called from carries: a build for this
      # machine needs none of what the dev shell's toolchain is there for.
      ahole =
        { lib, rustPlatform }:
        rustPlatform.buildRustPackage {
          pname = "ahole";
          version = (lib.importTOML ./Cargo.toml).package.version;

          # Only what cargo reads, so that editing the README or this file
          # does not rebuild the world.
          src = lib.fileset.toSource {
            root = ./.;
            fileset = lib.fileset.unions [
              ./Cargo.toml
              ./Cargo.lock
              ./src
              ./tests
            ];
          };

          # Every dependency comes from crates.io, so the lock file's own
          # checksums are enough and there is no vendor hash to keep in step.
          cargoLock.lockFile = ./Cargo.lock;

          meta = {
            description = "Send a file or directory to a friend, peer to peer, over iroh";
            homepage = "https://github.com/ast/ahole";
            license = with lib.licenses; [
              mit
              asl20
            ];
            mainProgram = "ahole";
            platforms = lib.platforms.unix;
          };
        };
    in
    {
      # `nixpkgs.overlays = [ inputs.ahole.overlays.default ];` puts `ahole` in
      # pkgs, built against the system's own nixpkgs rather than the one pinned
      # here — and for whatever architecture that system is.
      overlays.default = final: _prev: {
        ahole = final.callPackage ahole { };
      };

      packages.${system}.default = pkgs.callPackage ahole { };

      devShells.${system}.default = pkgs.mkShell {
        packages = [
          toolchain
          muslCc
          buildStatic
          pkgs.file
          pkgs.git
        ];

        # Only the musl target is steered here; plain `cargo build` and
        # `cargo test` keep building for this machine as usual.
        env = {
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER = "${muslCc}/bin/${muslPrefix}cc";
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS = "-C target-feature=+crt-static";
          CC_x86_64_unknown_linux_musl = "${muslCc}/bin/${muslPrefix}cc";
          AR_x86_64_unknown_linux_musl = "${muslCc.bintools}/bin/${muslPrefix}ar";
        };

        shellHook = ''
          echo "ahole dev shell — cargo $(cargo --version | cut -d' ' -f2), musl target ready"
          echo "  build-static    static binary for a friend (target/${target}/release/ahole)"
        '';
      };
    };
}
