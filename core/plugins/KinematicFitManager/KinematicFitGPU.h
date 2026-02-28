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
 * @brief Batch kinematic fit on the GPU.
 *
 * Launches a CUDA kernel that processes @p nEvents independent fits in
 * parallel.  All arrays are host (CPU) pointers; the function handles GPU
 * memory allocation, data transfer, kernel execution, and deallocation.
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
 * @param outputs    Host array [nEvents * (2 + nParticles*3)] — fit results (written by this function).
 */
void kinFitBatchGPU(const float *inputs, const float *sigmas, int nParticles,
                    const int *conTypes, const int *conIdx1, const int *conIdx2,
                    const int *conIdx3, const float *conTarget,
                    const float *conSigma, int nConstraints, int nEvents,
                    int maxIter, float tolerance, float *outputs);

#endif // USE_CUDA

#endif // KINEMATICFITGPU_H_INCLUDED
