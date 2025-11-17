{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { nixpkgs, flake-utils, fenix, ... }: flake-utils.lib.eachDefaultSystem (system:
    let 
      pkgs = import nixpkgs { inherit system; };
      
      # Cross-compilation setup for MinGW-w64
      crossPkgs = pkgs.pkgsCross.mingwW64;
      crossGcc = crossPkgs.buildPackages.gcc;
      crossPrefix = "x86_64-w64-mingw32";
      crossCompiler = "${crossGcc}/bin/${crossPrefix}";
    in {
      devShells.default = pkgs.mkShell {
        buildInputs = [
          fenix.packages.${system}.latest.toolchain
        ] ++ (with pkgs.python3Packages; [
          numpy
          pandas
          parquet
          pyarrow
        ]);
        LD_LIBRARY_PATH = with pkgs; lib.makeLibraryPath [ libGL wayland libxkbcommon vulkan-loader ];
      };
      devShells.win = pkgs.mkShell {
        nativeBuildInputs = [
          # Rust toolchain with Windows GNU target
          (with fenix.packages.${system}; combine [
            latest.toolchain
            targets.x86_64-pc-windows-gnu.latest.rust-std
          ])
          # MinGW-w64 cross-compiler ONLY
          crossGcc
        ];

        buildInputs = (with crossPkgs; [
          windows.pthreads
        ]);

        CC_x86_64_pc_windows_gnu = "${crossCompiler}-gcc";
        AR_x86_64_pc_windows_gnu = "${crossCompiler}-ar";
        
        # Cargo linker settings
        CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER = "${crossCompiler}-gcc";
          
        # Fallbacks
        # TARGET_CC = "${crossCompiler}-gcc";
        # TARGET_AR = "${crossCompiler}-ar";
      };
    }
  );
}

