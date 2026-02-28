#ifndef KINEMATICFITGPU_H_INCLUDED
#define KINEMATICFITGPU_H_INCLUDED

/**
 * @file KinematicFitGPU.h
 * @brief GPU-accelerated kinematic fit interface.
 *
 * This header declares the batch GPU kinematic fit function.  The
 * implementation lives in KinematicFitGPU.cu and is only compiled when the
 * project is built with @c -DUSE_CUDA=ON.
 *
 * **Batch-processing model**
 *
 * The GPU implementation processes @p nEvents independent kinematic fits in
 * parallel — one CUDA thread per event.  This is the natural model for HEP
 * analyses where the same fit topology (same set of particles and constraints)
 * is applied to every event in a dataset.
 *
 * **Maximum supported sizes**
 *
 * The CUDA kernel uses fixed-size device arrays to avoid dynamic GPU memory
 * allocation inside the kernel.  The compile-time limits are:
 *   - kGPUMaxParticles   = 10  (particles per fit)
 *   - kGPUMaxConstraints =  8  (constraints per fit, mass + pT combined)
 *
 * Fits that exceed these limits must use the CPU implementation.
 *
 * **Input/output layout**
 *
 * @p inputs (host) is a flat array of size @p nEvents × @p nParticles × 4.
 * For event @e e and particle @e i:
 * @code
 *   inputs[e * nParticles * 4 + i * 4 + 0] = pT   [GeV]
 *   inputs[e * nParticles * 4 + i * 4 + 1] = eta
 *   inputs[e * nParticles * 4 + i * 4 + 2] = phi   [rad]
 *   inputs[e * nParticles * 4 + i * 4 + 3] = mass  [GeV]
 * @endcode
 *
 * @p sigmas (host) is a flat array of size @p nParticles × 3 (same for every
 * event):
 * @code
 *   sigmas[i * 3 + 0] = fractional pT resolution  (sigma_pT / pT)
 *   sigmas[i * 3 + 1] = absolute eta resolution
 *   sigmas[i * 3 + 2] = absolute phi resolution [rad]
 * @endcode
 *
 * Constraint arrays (@p conTypes, @p conIdx1, @p conIdx2, @p conIdx3,
 * @p conTarget, @p conSigma) each have length @p nConstraints:
 * @code
 *   conTypes[c] = 0  ─ two-body invariant-mass constraint
 *   conTypes[c] = 1  ─ three-body invariant-mass constraint
 *   conTypes[c] = 2  ─ pT constraint on a single particle
 *   conIdx3[c]  = -1 for conTypes 0 and 2
 *   conSigma[c] > 0  converts a mass constraint to a soft (Gaussian) constraint
 *                    with resonance width conSigma [GeV]
 * @endcode
 *
 * @p outputs (host) is a flat array of size @p nEvents × (2 + @p nParticles × 3).
 * For event @e e:
 * @code
 *   outputs[e * outStride + 0]           = chi2
 *   outputs[e * outStride + 1]           = 1.0 if converged, else 0.0
 *   outputs[e * outStride + 2 + i*3 + 0] = fitted pT_i   [GeV]
 *   outputs[e * outStride + 2 + i*3 + 1] = fitted eta_i
 *   outputs[e * outStride + 2 + i*3 + 2] = fitted phi_i  [rad]
 * @endcode
 * where @c outStride = 2 + @p nParticles × 3.
 */

/// Maximum number of particles supported by the GPU kinematic-fit kernel.
static constexpr int kGPUMaxParticles   = 10;
/// Maximum number of constraints supported by the GPU kinematic-fit kernel.
static constexpr int kGPUMaxConstraints =  8;

#ifdef USE_CUDA

/**
 * @brief Persistent GPU context for a kinematic fit configuration.
 *
 * Holds device-side copies of the per-fit static data (per-particle
 * resolutions and constraint arrays) that are identical for every event.
 * Creating one context per fit instance and sharing it across all event
 * evaluations avoids repeated device memory allocations and H2D transfers
 * for data that never changes between events.
 *
 * Thread-safety: the device buffers are read-only after construction and
 * may be accessed concurrently from multiple CPU threads (RDataFrame slots).
 * Per-slot dynamic device buffers for the mutable per-event input/output data
 * are managed internally by kinFitRunGPU() via thread-local storage.
 *
 * Construct one context per fit when the analysis is configured (e.g. in
 * applyFit()), then capture it by shared_ptr in the per-event lambda.
 */
class CudaKinFitContext {
public:
  float *d_sigmas    = nullptr; ///< Device: [nParticles * 3] (sigPt, sigEta, sigPhi)
  int   *d_conTypes  = nullptr; ///< Device: [nConstraints] constraint types
  int   *d_conIdx1   = nullptr; ///< Device: [nConstraints] first particle index
  int   *d_conIdx2   = nullptr; ///< Device: [nConstraints] second particle index
  int   *d_conIdx3   = nullptr; ///< Device: [nConstraints] third particle index (-1 if N/A)
  float *d_conTarget = nullptr; ///< Device: [nConstraints] target mass/pT [GeV]
  float *d_conSigma  = nullptr; ///< Device: [nConstraints] resonance widths
  int   nParticles   = 0;
  int   nConstraints = 0;

  CudaKinFitContext(const float *sigmas, int nPart,
                    const int   *conTypes, const int   *conIdx1,
                    const int   *conIdx2,  const int   *conIdx3,
                    const float *conTarget, const float *conSigma, int nCon);
  ~CudaKinFitContext();

  CudaKinFitContext(const CudaKinFitContext &) = delete;
  CudaKinFitContext &operator=(const CudaKinFitContext &) = delete;
};

/**
 * @brief Run a batch of kinematic fits using a pre-allocated GPU context.
 *
 * Unlike kinFitBatchGPU(), this function reuses the static device buffers
 * held by @p ctx (resolutions and constraints — constant across events) and
 * internally maintains thread-local device buffers for the per-event
 * input/output data.  Thread-local buffers are grown on demand and reused
 * across calls within the same thread, so successive per-event calls avoid
 * repeated cudaMalloc / cudaFree overhead entirely.
 *
 * Intended use: create a CudaKinFitContext once when the fit is configured,
 * capture it by shared_ptr in the RDataFrame per-event lambda, and call
 * kinFitRunGPU() with nEvents=1 each invocation.
 *
 * @param ctx       Persistent fit context (static device data).
 * @param inputs    Host array [nEvents * nParticles * 4].
 * @param nEvents   Number of events to process.
 * @param maxIter   Maximum number of linearisation iterations.
 * @param tolerance Convergence criterion on |Δχ²|.
 * @param outputs   Host array [nEvents * (2 + nParticles*3)] — written here.
 */
void kinFitRunGPU(const CudaKinFitContext &ctx, const float *inputs,
                  int nEvents, int maxIter, float tolerance, float *outputs);

/**
 * @brief Batch kinematic fit on the GPU (standalone, allocates per call).
 *
 * Allocates and frees all device memory on every call.  Suitable for
 * standalone or large-batch usage where allocation cost is amortized over
 * many events per call.  For RDataFrame integration (nEvents=1 per call)
 * prefer kinFitRunGPU() with a persistent CudaKinFitContext.
 *
 * @param inputs     Host array [nEvents * nParticles * 4] — see layout above.
 * @param sigmas     Host array [nParticles * 3] — per-particle resolutions.
 * @param nParticles Number of particles per fit (≤ kGPUMaxParticles).
 * @param conTypes   Host array [nConstraints] — constraint types (0/1/2).
 * @param conIdx1    Host array [nConstraints] — first particle index.
 * @param conIdx2    Host array [nConstraints] — second particle index.
 * @param conIdx3    Host array [nConstraints] — third particle index (-1 if N/A).
 * @param conTarget  Host array [nConstraints] — target mass or pT [GeV].
 * @param conSigma   Host array [nConstraints] — resonance width for soft constraints.
 * @param nConstraints Number of constraints (≤ kGPUMaxConstraints).
 * @param nEvents    Number of events to process in this batch.
 * @param maxIter    Maximum number of linearisation iterations per fit.
 * @param tolerance  Convergence criterion on |Δχ²|.
 * @param outputs    Host array [nEvents * (2 + nParticles*3)] — fit results.
 */
void kinFitBatchGPU(const float *inputs, const float *sigmas, int nParticles,
                    const int *conTypes, const int *conIdx1, const int *conIdx2,
                    const int *conIdx3, const float *conTarget,
                    const float *conSigma, int nConstraints, int nEvents,
                    int maxIter, float tolerance, float *outputs);

#endif // USE_CUDA

#endif // KINEMATICFITGPU_H_INCLUDED
