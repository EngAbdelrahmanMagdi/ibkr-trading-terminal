package simulator

// Deterministic, stateless noise built only from integer arithmetic, so results are bit-identical on every
// platform and after every restart (no floating point, no shared random state).

// one is the fixed-point scale used by the noise functions (16 fractional bits).
const one = int64(1) << 16

// mix is the splitmix64 finalizer: a fast, well-distributed 64-bit hash.
func mix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// hash2 combines two values into one hash.
func hash2(a, b uint64) uint64 { return mix(a ^ mix(b)) }

// fnv64a hashes a string (FNV-1a, 64 bit).
func fnv64a(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// unit maps a hash to a fixed-point value in [-one, one).
func unit(h uint64) int64 { return int64(h>>47) - one }

// floorDiv divides rounding toward negative infinity.
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// valueNoise returns smooth noise in [-one, one] at time tMs for a lattice with spacing scaleMs.
// Lattice values come from hash2(key, index); between lattice points the value is interpolated with a
// smoothstep curve, so the function is continuous and its slope is bounded by 3*one/scaleMs per millisecond.
func valueNoise(key uint64, scaleMs, tMs int64) int64 {
	i := floorDiv(tMs, scaleMs)
	f := tMs - i*scaleMs   // [0, scaleMs)
	u := f * one / scaleMs // [0, one)
	s := u * u / one * (3*one - 2*u) / one
	n0 := unit(hash2(key, uint64(i)))
	n1 := unit(hash2(key, uint64(i+1)))
	return n0 + (n1-n0)*s/one
}
