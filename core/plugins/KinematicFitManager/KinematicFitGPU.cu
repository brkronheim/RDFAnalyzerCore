/**
 * @file KinematicFitGPU.cu
 * @brief CUDA implementation of the GPU-accelerated kinematic fit.
 *
 * Each CUDA thread processes one event independently.  The algorithm is a
 * direct port of the iterative Lagrange-multiplier method implemented on the
 * CPU in KinematicFit.h, using single-precision floating-point arithmetic and
 * fixed-size device arrays that fit entirely in registers / shared memory.
 *
 * Compile with:
 *   nvcc -arch=sm_XX -DUSE_CUDA -c KinematicFitGPU.cu
 * or let CMake handle it via the USE_CUDA build option.
 */

#include "KinematicFitGPU.h"

#include <cuda_runtime.h>
#include <stdexcept>
#include <string>

// ── compile-time limits ───────────────────────────────────────────────────────
// These must match the constants declared in KinematicFitGPU.h.
static_assert(kGPUMaxParticles   <= 10, "kGPUMaxParticles too large");
static_assert(kGPUMaxConstraints <=  8, "kGPUMaxConstraints too large");

namespace {

constexpr int kMaxParams = kGPUMaxParticles * 3; // 30

// Numerical constants (GPU-side)
constexpr float kMinEnergy_g      = 1e-9f;
constexpr float kMinVariance_g    = 1e-12f;
constexpr float kSingularityEps_g = 1e-30f;
constexpr float kMinPt_g          = 1e-4f;

// ── Thread-local per-slot dynamic device buffers ────────────────────────────
//
// Each RDataFrame slot runs on its own OS thread.  The first call from a slot
// allocates input/output device buffers; subsequent calls reuse them.  Buffers
// grow when a larger event batch is requested and are freed when the thread
// exits, so no per-event cudaMalloc/cudaFree overhead occurs after warm-up.
struct GpuDynSlot {
  float *d_inputs  = nullptr;
  float *d_outputs = nullptr;
  size_t cap_in    = 0; // allocated bytes for d_inputs
  size_t cap_out   = 0; // allocated bytes for d_outputs

  /// Ensure device buffers are large enough; (re-)allocates only when needed.
  void ensure(size_t need_in, size_t need_out) {
    if (need_in > cap_in) {
      cudaFree(d_inputs);
      d_inputs = nullptr;
      if (cudaMalloc(&d_inputs, need_in) == cudaSuccess)
        cap_in = need_in;
      else
        cap_in = 0;
    }
    if (need_out > cap_out) {
      cudaFree(d_outputs);
      d_outputs = nullptr;
      if (cudaMalloc(&d_outputs, need_out) == cudaSuccess)
        cap_out = need_out;
      else
        cap_out = 0;
    }
  }

  ~GpuDynSlot() {
    // cudaFree is a no-op when the CUDA driver has already been unloaded.
    cudaFree(d_inputs);
    cudaFree(d_outputs);
  }
};

thread_local GpuDynSlot gpuDynSlot;

// ── CUDA error-checking helper ──────────────────────────────────────────────

void checkCuda(cudaError_t err, const char *location) {
  if (err != cudaSuccess) {
    throw std::runtime_error(std::string("CUDA error at ") + location + ": " +
                             cudaGetErrorString(err));
  }
}

} // anonymous namespace

// ── device helper: compute (E, px, py, pz) from (pT, eta, phi, mass) ─────────

__device__ static void d_fourMomentum(float pt, float eta, float phi, float mass,
                                       float &E, float &px, float &py, float &pz) {
  const float ch = coshf(eta);
  const float sh = sinhf(eta);
  E  = sqrtf(pt * pt * ch * ch + mass * mass);
  px = pt * cosf(phi);
  py = pt * sinf(phi);
  pz = pt * sh;
}

// ── CUDA kernel: one thread per event ─────────────────────────────────────────

/**
 * @brief Per-event kinematic fit kernel.
 *
 * Each thread independently runs the iterative Lagrange-multiplier fit for
 * one event.  Fixed-size arrays (sized by the compile-time limits) hold the
 * fit state so no dynamic device memory is needed.
 *
 * chi2 is always recomputed from the final particle state after the iteration
 * loop, regardless of why the loop ended (convergence, maximum iterations, or
 * a singular matrix).  This matches the CPU implementation.
 */
__global__ static void kinFitKernel(
    const float *__restrict__ inputs,    ///< [nEvents * nParticles * 4]
    const float *__restrict__ sigmas,    ///< [nParticles * 3]: sigPt, sigEta, sigPhi
    const int   *__restrict__ conTypes,  ///< [nConstraints]: 0=mass2, 1=mass3, 2=pt
    const int   *__restrict__ conIdx1,   ///< [nConstraints]
    const int   *__restrict__ conIdx2,   ///< [nConstraints]
    const int   *__restrict__ conIdx3,   ///< [nConstraints]
    const float *__restrict__ conTarget, ///< [nConstraints]
    const float *__restrict__ conSigma,  ///< [nConstraints]
    float       *__restrict__ outputs,   ///< [nEvents * (2 + nParticles*3)]
    int nParticles, int nConstraints,
    int nEvents, int maxIter, float tolerance) {

  const int ev = static_cast<int>(blockIdx.x) * static_cast<int>(blockDim.x) +
                 static_cast<int>(threadIdx.x);
  if (ev >= nEvents) return;

  const int nParams    = nParticles * 3;
  const int outStride  = 2 + nParticles * 3;

  // ── load particle data for this event ───────────────────────────────────────
  float pt  [kGPUMaxParticles];
  float eta [kGPUMaxParticles];
  float phi [kGPUMaxParticles];
  float mass[kGPUMaxParticles];
  float origPt [kGPUMaxParticles];
  float origEta[kGPUMaxParticles];
  float origPhi[kGPUMaxParticles];

  const float *evIn = inputs + ev * nParticles * 4;
  for (int i = 0; i < nParticles; ++i) {
    pt  [i] = origPt [i] = evIn[i * 4 + 0];
    eta [i] = origEta[i] = evIn[i * 4 + 1];
    phi [i] = origPhi[i] = evIn[i * 4 + 2];
    mass[i]              = evIn[i * 4 + 3];
  }

  // ── build per-event variance vector ─────────────────────────────────────────
  // sigmas[i*3+0] is the *fractional* pT resolution, so var_pT = (sigPt * pT)^2.
  float var[kMaxParams];
  for (int i = 0; i < nParticles; ++i) {
    const float sp = sigmas[i * 3 + 0] * pt[i];
    var[3 * i + 0] = fmaxf(sp * sp, kMinVariance_g);
    var[3 * i + 1] = fmaxf(sigmas[i * 3 + 1] * sigmas[i * 3 + 1], kMinVariance_g);
    var[3 * i + 2] = fmaxf(sigmas[i * 3 + 2] * sigmas[i * 3 + 2], kMinVariance_g);
  }

  float prevChi2 = 1e30f;
  bool  converged = false;

  // Working arrays (sized to compile-time limits)
  float f  [kGPUMaxConstraints];
  float D  [kGPUMaxConstraints * kMaxParams]; // row-major: D[c * nParams + k]
  float W  [kGPUMaxConstraints * kGPUMaxConstraints];
  float lam[kGPUMaxConstraints];

  for (int iter = 0; iter < maxIter; ++iter) {

    // ── initialise f and D ───────────────────────────────────────────────────
    for (int c = 0; c < nConstraints; ++c) f[c] = 0.f;
    for (int c = 0; c < nConstraints * nParams; ++c) D[c] = 0.f;

    // ── fill f and D for each constraint ────────────────────────────────────
    for (int c = 0; c < nConstraints; ++c) {
      const int type = conTypes[c];

      if (type == 2) {
        // pT constraint: f = pT_i - target;  D[c, i*3+0] = 1
        const int i = conIdx1[c];
        f[c] = pt[i] - conTarget[c];
        D[c * nParams + i * 3 + 0] = 1.f;

      } else if (type == 0) {
        // Two-body invariant-mass constraint
        const int i1 = conIdx1[c], i2 = conIdx2[c];
        float E1, px1, py1, pz1, E2, px2, py2, pz2;
        d_fourMomentum(pt[i1], eta[i1], phi[i1], mass[i1], E1, px1, py1, pz1);
        d_fourMomentum(pt[i2], eta[i2], phi[i2], mass[i2], E2, px2, py2, pz2);
        const float sE  = E1  + E2;
        const float sPx = px1 + px2;
        const float sPy = py1 + py2;
        const float sPz = pz1 + pz2;
        const float mt  = conTarget[c];
        f[c] = sE * sE - sPx * sPx - sPy * sPy - sPz * sPz - mt * mt;

        // Gradient for each of the two particles
        for (int k = 0; k < 2; ++k) {
          const int    ik  = (k == 0) ? i1  : i2;
          const float  Ek  = (k == 0) ? E1  : E2;
          const float  chk = coshf(eta[ik]);
          const float  shk = sinhf(eta[ik]);
          const float dEk_dpt  = (Ek > kMinEnergy_g) ? pt[ik]*chk*chk/Ek : 0.f;
          const float dEk_deta = (Ek > kMinEnergy_g) ? pt[ik]*pt[ik]*shk*chk/Ek : 0.f;
          D[c * nParams + ik * 3 + 0] = 2.f * (sE  * dEk_dpt
                                                - sPx * cosf(phi[ik])
                                                - sPy * sinf(phi[ik])
                                                - sPz * shk);
          D[c * nParams + ik * 3 + 1] = 2.f * (sE  * dEk_deta
                                                - sPz * pt[ik] * chk);
          D[c * nParams + ik * 3 + 2] = 2.f * (sPx * pt[ik] * sinf(phi[ik])
                                                - sPy * pt[ik] * cosf(phi[ik]));
        }

      } else {
        // Three-body invariant-mass constraint (type == 1)
        const int i1 = conIdx1[c], i2 = conIdx2[c], i3 = conIdx3[c];
        float E1, px1, py1, pz1, E2, px2, py2, pz2, E3, px3, py3, pz3;
        d_fourMomentum(pt[i1], eta[i1], phi[i1], mass[i1], E1, px1, py1, pz1);
        d_fourMomentum(pt[i2], eta[i2], phi[i2], mass[i2], E2, px2, py2, pz2);
        d_fourMomentum(pt[i3], eta[i3], phi[i3], mass[i3], E3, px3, py3, pz3);
        const float sE  = E1  + E2  + E3;
        const float sPx = px1 + px2 + px3;
        const float sPy = py1 + py2 + py3;
        const float sPz = pz1 + pz2 + pz3;
        const float mt  = conTarget[c];
        f[c] = sE * sE - sPx * sPx - sPy * sPy - sPz * sPz - mt * mt;

        // Gradient for each of the three particles
        const int  idx3[3] = {i1, i2, i3};
        const float Ev3[3] = {E1, E2, E3};
        for (int k = 0; k < 3; ++k) {
          const int    ik  = idx3[k];
          const float  Ek  = Ev3[k];
          const float  chk = coshf(eta[ik]);
          const float  shk = sinhf(eta[ik]);
          const float dEk_dpt  = (Ek > kMinEnergy_g) ? pt[ik]*chk*chk/Ek : 0.f;
          const float dEk_deta = (Ek > kMinEnergy_g) ? pt[ik]*pt[ik]*shk*chk/Ek : 0.f;
          D[c * nParams + ik * 3 + 0] = 2.f * (sE  * dEk_dpt
                                                - sPx * cosf(phi[ik])
                                                - sPy * sinf(phi[ik])
                                                - sPz * shk);
          D[c * nParams + ik * 3 + 1] = 2.f * (sE  * dEk_deta
                                                - sPz * pt[ik] * chk);
          D[c * nParams + ik * 3 + 2] = 2.f * (sPx * pt[ik] * sinf(phi[ik])
                                                - sPy * pt[ik] * cosf(phi[ik]));
        }
      }
    } // for c

    // ── W = D * diag(var) * D^T ──────────────────────────────────────────────
    for (int ci = 0; ci < nConstraints; ++ci) {
      for (int cj = 0; cj < nConstraints; ++cj) {
        float w = 0.f;
        for (int k = 0; k < nParams; ++k) {
          w += D[ci * nParams + k] * var[k] * D[cj * nParams + k];
        }
        W[ci * nConstraints + cj] = w;
      }
    }

    // ── soften mass constraints ──────────────────────────────────────────────
    for (int c = 0; c < nConstraints; ++c) {
      if (conTypes[c] != 2 && conSigma[c] > 0.f) {
        const float rs = 2.f * conTarget[c] * conSigma[c];
        W[c * nConstraints + c] += rs * rs;
      }
    }

    // ── solve W * lam = -f ───────────────────────────────────────────────────
    // On singularity we break immediately; chi2 is computed fresh from the
    // current particle state after the loop, matching the CPU behaviour.
    if (nConstraints == 1) {
      if (fabsf(W[0]) < kSingularityEps_g) break;
      lam[0] = -f[0] / W[0];

    } else if (nConstraints == 2) {
      const float det = W[0] * W[3] - W[1] * W[2];
      if (fabsf(det) < kSingularityEps_g) break;
      lam[0] = (-f[0] * W[3] + f[1] * W[1]) / det;
      lam[1] = (-f[1] * W[0] + f[0] * W[2]) / det;

    } else {
      // General Gauss elimination with partial pivoting
      float aug[kGPUMaxConstraints * (kGPUMaxConstraints + 1)];
      for (int r = 0; r < nConstraints; ++r) {
        for (int col = 0; col < nConstraints; ++col) {
          aug[r * (nConstraints + 1) + col] = W[r * nConstraints + col];
        }
        aug[r * (nConstraints + 1) + nConstraints] = -f[r];
      }
      for (int col = 0; col < nConstraints; ++col) {
        int pivot = col;
        for (int r = col + 1; r < nConstraints; ++r) {
          if (fabsf(aug[r * (nConstraints + 1) + col]) >
              fabsf(aug[pivot * (nConstraints + 1) + col])) {
            pivot = r;
          }
        }
        if (pivot != col) {
          for (int k = 0; k <= nConstraints; ++k) {
            const float tmp = aug[col * (nConstraints + 1) + k];
            aug[col   * (nConstraints + 1) + k] =
                aug[pivot * (nConstraints + 1) + k];
            aug[pivot * (nConstraints + 1) + k] = tmp;
          }
        }
        const float diag = aug[col * (nConstraints + 1) + col];
        if (fabsf(diag) < kSingularityEps_g) break;
        for (int r = col + 1; r < nConstraints; ++r) {
          const float factor = aug[r * (nConstraints + 1) + col] / diag;
          for (int k = col; k <= nConstraints; ++k) {
            aug[r * (nConstraints + 1) + k] -=
                factor * aug[col * (nConstraints + 1) + k];
          }
        }
      }
      // Back substitution
      for (int r = nConstraints - 1; r >= 0; --r) {
        float sum = aug[r * (nConstraints + 1) + nConstraints];
        for (int k = r + 1; k < nConstraints; ++k) {
          sum -= aug[r * (nConstraints + 1) + k] * lam[k];
        }
        const float diag = aug[r * (nConstraints + 1) + r];
        lam[r] = (fabsf(diag) > kSingularityEps_g) ? sum / diag : 0.f;
      }
    }

    // ── update particles: delta = V * D^T * lambda ───────────────────────────
    for (int i = 0; i < nParticles; ++i) {
      float dpt = 0.f, deta = 0.f, dphi = 0.f;
      for (int c = 0; c < nConstraints; ++c) {
        dpt  += var[3*i+0] * D[c*nParams + 3*i+0] * lam[c];
        deta += var[3*i+1] * D[c*nParams + 3*i+1] * lam[c];
        dphi += var[3*i+2] * D[c*nParams + 3*i+2] * lam[c];
      }
      pt [i] = fmaxf(pt[i] + dpt, kMinPt_g);
      eta[i] += deta;
      phi[i] += dphi;
    }

    // ── compute chi2 from current state ─────────────────────────────────────
    float chi2 = 0.f;
    for (int i = 0; i < nParticles; ++i) {
      const float dpt  = pt [i] - origPt [i];
      const float deta = eta[i] - origEta[i];
      const float dphi = phi[i] - origPhi[i];
      chi2 += dpt  * dpt  / var[3*i+0];
      chi2 += deta * deta / var[3*i+1];
      chi2 += dphi * dphi / var[3*i+2];
    }

    if (fabsf(chi2 - prevChi2) < tolerance) {
      prevChi2  = chi2;
      converged = true;
      break;
    }
    prevChi2 = chi2;
  } // for iter

  // ── write outputs ─────────────────────────────────────────────────────────
  // Always recompute chi2 from the final particle state (matches CPU behaviour
  // where chi2 is computed after the loop regardless of exit reason).  This
  // also handles the singularity-break case correctly: prevChi2 would be 1e30
  // if singularity occurred on the first iteration with no particle updates.
  float finalChi2 = 0.f;
  for (int i = 0; i < nParticles; ++i) {
    const float dpt  = pt [i] - origPt [i];
    const float deta = eta[i] - origEta[i];
    const float dphi = phi[i] - origPhi[i];
    finalChi2 += dpt  * dpt  / var[3*i+0];
    finalChi2 += deta * deta / var[3*i+1];
    finalChi2 += dphi * dphi / var[3*i+2];
  }

  float *evOut = outputs + ev * outStride;
  evOut[0] = finalChi2;
  evOut[1] = converged ? 1.f : 0.f;
  for (int i = 0; i < nParticles; ++i) {
    evOut[2 + i * 3 + 0] = pt [i];
    evOut[2 + i * 3 + 1] = eta[i];
    evOut[2 + i * 3 + 2] = phi[i];
  }
}

// ── CudaKinFitContext ─────────────────────────────────────────────────────────

CudaKinFitContext::CudaKinFitContext(
    const float *sigmas, int nPart,
    const int   *conTypes, const int   *conIdx1,
    const int   *conIdx2,  const int   *conIdx3,
    const float *conTarget, const float *conSigma, int nCon)
    : nParticles(nPart), nConstraints(nCon) {
  try {
    checkCuda(cudaMalloc(&d_sigmas, static_cast<size_t>(nPart) * 3 * sizeof(float)),
              "CudaKinFitContext: malloc sigmas");
    if (nCon > 0) {
      checkCuda(cudaMalloc(&d_conTypes,  static_cast<size_t>(nCon) * sizeof(int)),   "malloc conTypes");
      checkCuda(cudaMalloc(&d_conIdx1,   static_cast<size_t>(nCon) * sizeof(int)),   "malloc conIdx1");
      checkCuda(cudaMalloc(&d_conIdx2,   static_cast<size_t>(nCon) * sizeof(int)),   "malloc conIdx2");
      checkCuda(cudaMalloc(&d_conIdx3,   static_cast<size_t>(nCon) * sizeof(int)),   "malloc conIdx3");
      checkCuda(cudaMalloc(&d_conTarget, static_cast<size_t>(nCon) * sizeof(float)), "malloc conTarget");
      checkCuda(cudaMalloc(&d_conSigma,  static_cast<size_t>(nCon) * sizeof(float)), "malloc conSigma");
    }

    checkCuda(cudaMemcpy(d_sigmas, sigmas,
                         static_cast<size_t>(nPart) * 3 * sizeof(float),
                         cudaMemcpyHostToDevice), "H2D sigmas");
    if (nCon > 0) {
      checkCuda(cudaMemcpy(d_conTypes,  conTypes,  static_cast<size_t>(nCon) * sizeof(int),   cudaMemcpyHostToDevice), "H2D conTypes");
      checkCuda(cudaMemcpy(d_conIdx1,   conIdx1,   static_cast<size_t>(nCon) * sizeof(int),   cudaMemcpyHostToDevice), "H2D conIdx1");
      checkCuda(cudaMemcpy(d_conIdx2,   conIdx2,   static_cast<size_t>(nCon) * sizeof(int),   cudaMemcpyHostToDevice), "H2D conIdx2");
      checkCuda(cudaMemcpy(d_conIdx3,   conIdx3,   static_cast<size_t>(nCon) * sizeof(int),   cudaMemcpyHostToDevice), "H2D conIdx3");
      checkCuda(cudaMemcpy(d_conTarget, conTarget, static_cast<size_t>(nCon) * sizeof(float), cudaMemcpyHostToDevice), "H2D conTarget");
      checkCuda(cudaMemcpy(d_conSigma,  conSigma,  static_cast<size_t>(nCon) * sizeof(float), cudaMemcpyHostToDevice), "H2D conSigma");
    }
  } catch (...) {
    cudaFree(d_sigmas);   d_sigmas   = nullptr;
    cudaFree(d_conTypes); d_conTypes = nullptr;
    cudaFree(d_conIdx1);  d_conIdx1  = nullptr;
    cudaFree(d_conIdx2);  d_conIdx2  = nullptr;
    cudaFree(d_conIdx3);  d_conIdx3  = nullptr;
    cudaFree(d_conTarget); d_conTarget = nullptr;
    cudaFree(d_conSigma);  d_conSigma  = nullptr;
    throw;
  }
}

CudaKinFitContext::~CudaKinFitContext() {
  cudaFree(d_sigmas);
  cudaFree(d_conTypes);
  cudaFree(d_conIdx1);
  cudaFree(d_conIdx2);
  cudaFree(d_conIdx3);
  cudaFree(d_conTarget);
  cudaFree(d_conSigma);
}

// ── kinFitRunGPU ──────────────────────────────────────────────────────────────

void kinFitRunGPU(const CudaKinFitContext &ctx, const float *inputs,
                  int nEvents, int maxIter, float tolerance, float *outputs) {
  if (nEvents <= 0) return;

  const int    nParticles   = ctx.nParticles;
  const int    nConstraints = ctx.nConstraints;
  const int    outStride    = 2 + nParticles * 3;
  const size_t in_bytes     = static_cast<size_t>(nEvents) * nParticles * 4 * sizeof(float);
  const size_t out_bytes    = static_cast<size_t>(nEvents) * outStride * sizeof(float);

  // Ensure thread-local dynamic buffers are large enough.
  gpuDynSlot.ensure(in_bytes, out_bytes);
  if (!gpuDynSlot.d_inputs || !gpuDynSlot.d_outputs) {
    throw std::runtime_error("kinFitRunGPU: failed to allocate device I/O buffers");
  }

  checkCuda(cudaMemcpy(gpuDynSlot.d_inputs, inputs, in_bytes, cudaMemcpyHostToDevice),
            "kinFitRunGPU: H2D inputs");

  constexpr int kBlockSize = 256;
  const int gridSize = (nEvents + kBlockSize - 1) / kBlockSize;

  kinFitKernel<<<gridSize, kBlockSize>>>(
      gpuDynSlot.d_inputs, ctx.d_sigmas,
      ctx.d_conTypes, ctx.d_conIdx1, ctx.d_conIdx2, ctx.d_conIdx3,
      ctx.d_conTarget, ctx.d_conSigma,
      gpuDynSlot.d_outputs,
      nParticles, nConstraints, nEvents, maxIter, tolerance);

  checkCuda(cudaGetLastError(),      "kinFitRunGPU: kernel launch");
  checkCuda(cudaDeviceSynchronize(), "kinFitRunGPU: sync");

  checkCuda(cudaMemcpy(outputs, gpuDynSlot.d_outputs, out_bytes, cudaMemcpyDeviceToHost),
            "kinFitRunGPU: D2H outputs");
}

// ── kinFitBatchGPU (standalone / large-batch) ────────────────────────────────

void kinFitBatchGPU(const float *inputs, const float *sigmas, int nParticles,
                    const int *conTypes, const int *conIdx1, const int *conIdx2,
                    const int *conIdx3, const float *conTarget,
                    const float *conSigma, int nConstraints, int nEvents,
                    int maxIter, float tolerance, float *outputs) {
  if (nParticles > kGPUMaxParticles) {
    throw std::runtime_error(
        "kinFitBatchGPU: nParticles (" + std::to_string(nParticles) +
        ") exceeds kGPUMaxParticles (" + std::to_string(kGPUMaxParticles) + ")");
  }
  if (nConstraints > kGPUMaxConstraints) {
    throw std::runtime_error(
        "kinFitBatchGPU: nConstraints (" + std::to_string(nConstraints) +
        ") exceeds kGPUMaxConstraints (" +
        std::to_string(kGPUMaxConstraints) + ")");
  }
  if (nEvents <= 0) return;

  // Build a context (uploads static data) then run; context is freed on scope exit.
  CudaKinFitContext ctx(sigmas, nParticles, conTypes, conIdx1, conIdx2, conIdx3,
                        conTarget, conSigma, nConstraints);
  kinFitRunGPU(ctx, inputs, nEvents, maxIter, tolerance, outputs);
}

