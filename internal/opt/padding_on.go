//go:build synx_enable_padding || (!synx_disable_padding && (arm64 || loong64 || mips64 || mips64le || ppc64 || ppc64le || riscv64 || s390x))

package opt

const PaddingMult_ = 1
