#ifndef KINEMATICFIT_H_INCLUDED
#define KINEMATICFIT_H_INCLUDED

#include <array>
#include <cmath>
#include <stdexcept>
#include <vector>

/**
 * @brief Represents a particle in the kinematic fit with measurement
 *        uncertainties on its kinematic parameters.
 */
struct KinFitParticle {
  double pt;    ///< Transverse momentum [GeV]
  double eta;   ///< Pseudorapidity
  double phi;   ///< Azimuthal angle [rad]
  double mass;  ///< Particle mass [GeV] (fixed during fit)
  double sigPt;  ///< Fractional pT resolution  (sigma_pT / pT)
  double sigEta; ///< Absolute eta resolution
  double sigPhi; ///< Absolute phi resolution
};

/**
 * @brief Result returned by a kinematic fit.
 */
struct KinFitResult {
  std::vector<KinFitParticle> fittedParticles; ///< Fitted particle parameters
  double chi2;        ///< Chi-square at the fitted point
  int nIterations;    ///< Number of iterations used
  bool converged;     ///< Whether the fit converged within tolerance
};

/**
 * @brief Two-body invariant mass constraint.
 */
struct MassConstraint {
  int idx1;          ///< Index of the first particle
  int idx2;          ///< Index of the second particle
  double targetMass; ///< Target invariant mass [GeV]
};

/// @cond INTERNAL
namespace detail {
/// Minimum energy guard to avoid division by zero in the Jacobian.
constexpr double kMinEnergy      = 1e-9;
/// Floor on per-parameter variance (prevents ill-conditioning).
constexpr double kMinVariance    = 1e-12;
/// Near-zero threshold for matrix singularity checks.
constexpr double kSingularityEps = 1e-30;
/// Minimum allowed fitted pT [GeV] (keeps particles physical).
constexpr double kMinPt          = 1e-4;
} // namespace detail
/// @endcond

/**
 * @class KinematicFit
 * @brief Kinematic fitter using iterative linearization with Lagrange multipliers.
 *
 * Minimises
 *   chi2 = sum_i [ (delta_pT_i / sigma_pT_i)^2
 *                + (delta_eta_i / sigma_eta_i)^2
 *                + (delta_phi_i / sigma_phi_i)^2 ]
 * subject to two-body invariant mass constraints f_c(particles) = 0.
 *
 * The method follows the standard HEP approach (Lagrange multipliers with
 * linearisation) applicable to any number of constraints; a direct 2x2
 * inversion is used for 1–2 constraints while Gaussian elimination handles
 * more than 2.  For each iteration:
 *   delta_eta = V * D^T * lambda
 *   lambda    = -(D * V * D^T)^{-1} * f
 * where D_{ci} = df_c/deta_i evaluated at the current point.
 *
 * Usage example (dilepton+dijet):
 * @code
 *   KinematicFit fitter;
 *   int iL1 = fitter.addParticle({l1pt, l1eta, l1phi, 0.106, 0.02, 0.001, 0.001});
 *   int iL2 = fitter.addParticle({l2pt, l2eta, l2phi, 0.106, 0.02, 0.001, 0.001});
 *   int iJ1 = fitter.addParticle({j1pt, j1eta, j1phi, 0.0,   0.10, 0.05,  0.05});
 *   int iJ2 = fitter.addParticle({j2pt, j2eta, j2phi, 0.0,   0.10, 0.05,  0.05});
 *   fitter.addMassConstraint(iL1, iL2, 91.2);   // Z -> ll
 *   fitter.addMassConstraint(iJ1, iJ2, 125.0);  // H -> bb
 *   KinFitResult result = fitter.fit();
 * @endcode
 */
class KinematicFit {
public:
  /**
   * @brief Add a particle to the fit.
   * @param p Particle with measured 4-momentum and per-parameter resolutions.
   * @return Index of the added particle (for use in addMassConstraint).
   */
  int addParticle(const KinFitParticle &p) {
    particles_.push_back(p);
    return static_cast<int>(particles_.size()) - 1;
  }

  /**
   * @brief Add a two-body invariant mass constraint.
   * @param idx1       Index of the first particle.
   * @param idx2       Index of the second particle.
   * @param targetMass Target invariant mass [GeV].
   */
  void addMassConstraint(int idx1, int idx2, double targetMass) {
    constraints_.push_back({idx1, idx2, targetMass});
  }

  /**
   * @brief Perform the kinematic fit.
   * @param maxIter   Maximum number of linearisation iterations.
   * @param tolerance Convergence criterion on |delta_chi2|.
   * @return KinFitResult with fitted particle parameters and chi-square.
   */
  KinFitResult fit(int maxIter = 50, double tolerance = 1e-6) const;

private:
  std::vector<KinFitParticle> particles_;
  std::vector<MassConstraint> constraints_;

  // ── helpers ────────────────────────────────────────────────────────────────

  /// Compute (E, px, py, pz) from (pT, eta, phi, mass).
  static std::array<double, 4> fourMomentum(double pt, double eta, double phi,
                                             double mass) {
    const double chEta = std::cosh(eta);
    const double shEta = std::sinh(eta);
    const double E  = std::sqrt(pt * pt * chEta * chEta + mass * mass);
    const double px = pt * std::cos(phi);
    const double py = pt * std::sin(phi);
    const double pz = pt * shEta;
    return {E, px, py, pz};
  }

  /// Compute (m12)^2 for two particles in their current state.
  static double invMass2(const KinFitParticle &p1, const KinFitParticle &p2) {
    auto [E1, px1, py1, pz1] = fourMomentum(p1.pt, p1.eta, p1.phi, p1.mass);
    auto [E2, px2, py2, pz2] = fourMomentum(p2.pt, p2.eta, p2.phi, p2.mass);
    const double dE  = E1  + E2;
    const double dpx = px1 + px2;
    const double dpy = py1 + py2;
    const double dpz = pz1 + pz2;
    return dE * dE - dpx * dpx - dpy * dpy - dpz * dpz;
  }

  /**
   * @brief Gradient of (m12)^2 w.r.t. (pT, eta, phi) of particle @p p1,
   *        given the other particle @p p2 at its current parameters.
   * @return {d(m12^2)/dpT1, d(m12^2)/deta1, d(m12^2)/dphi1}
   */
  static std::array<double, 3> gradMass2(const KinFitParticle &p1,
                                          const KinFitParticle &p2) {
    const double ch1 = std::cosh(p1.eta);
    const double sh1 = std::sinh(p1.eta);
    auto [E1, px1, py1, pz1] = fourMomentum(p1.pt, p1.eta, p1.phi, p1.mass);
    auto [E2, px2, py2, pz2] = fourMomentum(p2.pt, p2.eta, p2.phi, p2.mass);

    // Partial derivatives of p1's 4-momentum w.r.t. (pT1, eta1, phi1)
    const double dE1_dpT  = (E1 > detail::kMinEnergy) ? p1.pt * ch1 * ch1 / E1 : 0.0;
    const double dE1_deta = (E1 > detail::kMinEnergy) ? p1.pt * p1.pt * sh1 * ch1 / E1 : 0.0;
    const double dE1_dphi = 0.0;

    const double dpx1_dpT  =  std::cos(p1.phi);
    const double dpx1_deta =  0.0;
    const double dpx1_dphi = -p1.pt * std::sin(p1.phi);

    const double dpy1_dpT  =  std::sin(p1.phi);
    const double dpy1_deta =  0.0;
    const double dpy1_dphi =  p1.pt * std::cos(p1.phi);

    const double dpz1_dpT  =  sh1;
    const double dpz1_deta =  p1.pt * ch1;
    const double dpz1_dphi =  0.0;

    // d(m12^2)/dxi = 2*[(E1+E2)*dE1/dxi - (px1+px2)*dpx1/dxi
    //                  - (py1+py2)*dpy1/dxi - (pz1+pz2)*dpz1/dxi]
    const double sE  = E1  + E2;
    const double spx = px1 + px2;
    const double spy = py1 + py2;
    const double spz = pz1 + pz2;

    const double d_dpT  = 2.0 * (sE * dE1_dpT  - spx * dpx1_dpT  -
                                  spy * dpy1_dpT  - spz * dpz1_dpT);
    const double d_deta = 2.0 * (sE * dE1_deta - spx * dpx1_deta -
                                  spy * dpy1_deta - spz * dpz1_deta);
    const double d_dphi = 2.0 * (sE * dE1_dphi - spx * dpx1_dphi -
                                  spy * dpy1_dphi - spz * dpz1_dphi);
    return {d_dpT, d_deta, d_dphi};
  }
};

// ── KinematicFit::fit implementation (header-only) ────────────────────────

inline KinFitResult KinematicFit::fit(int maxIter, double tolerance) const {
  const int nParticles   = static_cast<int>(particles_.size());
  const int nConstraints = static_cast<int>(constraints_.size());
  const int nParams      = nParticles * 3; // (pT, eta, phi) per particle

  if (nParticles == 0 || nConstraints == 0) {
    return {particles_, 0.0, 0, true};
  }

  // ── build diagonal variance vector ──────────────────────────────────────
  std::vector<double> var(nParams);
  for (int i = 0; i < nParticles; ++i) {
    // pT variance: use (sigma_pT * pT)^2 (fractional resolution)
    const double sp = particles_[i].sigPt * particles_[i].pt;
    var[3 * i + 0] = std::max(sp * sp, detail::kMinVariance);
    // eta/phi variances: absolute
    var[3 * i + 1] = std::max(particles_[i].sigEta * particles_[i].sigEta, detail::kMinVariance);
    var[3 * i + 2] = std::max(particles_[i].sigPhi * particles_[i].sigPhi, detail::kMinVariance);
  }

  // Working copy of particle parameters
  std::vector<KinFitParticle> current = particles_;
  double prevChi2 = 1e30;

  for (int iter = 0; iter < maxIter; ++iter) {
    // ── build constraint vector f and Jacobian D ─────────────────────────
    std::vector<double> f(nConstraints, 0.0);
    // D is (nConstraints × nParams), stored row-major
    std::vector<double> D(nConstraints * nParams, 0.0);

    for (int c = 0; c < nConstraints; ++c) {
      const auto &con = constraints_[c];
      const auto &p1  = current[con.idx1];
      const auto &p2  = current[con.idx2];

      f[c] = invMass2(p1, p2) - con.targetMass * con.targetMass;

      auto g1 = gradMass2(p1, p2);
      auto g2 = gradMass2(p2, p1); // gradient w.r.t. p2 (swap arguments)

      const int i1 = con.idx1 * 3;
      const int i2 = con.idx2 * 3;
      D[c * nParams + i1 + 0] = g1[0];
      D[c * nParams + i1 + 1] = g1[1];
      D[c * nParams + i1 + 2] = g1[2];
      D[c * nParams + i2 + 0] = g2[0];
      D[c * nParams + i2 + 1] = g2[1];
      D[c * nParams + i2 + 2] = g2[2];
    }

    // ── compute W = D * V * D^T  (nConstraints × nConstraints) ──────────
    std::vector<double> W(nConstraints * nConstraints, 0.0);
    for (int ci = 0; ci < nConstraints; ++ci) {
      for (int cj = 0; cj < nConstraints; ++cj) {
        double w = 0.0;
        for (int k = 0; k < nParams; ++k) {
          w += D[ci * nParams + k] * var[k] * D[cj * nParams + k];
        }
        W[ci * nConstraints + cj] = w;
      }
    }

    // ── solve  W * lambda = -f  ──────────────────────────────────────────
    std::vector<double> lambda(nConstraints, 0.0);
    if (nConstraints == 1) {
      if (std::abs(W[0]) < detail::kSingularityEps) break;
      lambda[0] = -f[0] / W[0];
    } else if (nConstraints == 2) {
      const double det = W[0] * W[3] - W[1] * W[2];
      if (std::abs(det) < detail::kSingularityEps) break;
      lambda[0] = (-f[0] * W[3] + f[1] * W[1]) / det;
      lambda[1] = (-f[1] * W[0] + f[0] * W[2]) / det;
    } else {
      // General Gauss elimination for nConstraints > 2
      // Augmented matrix [W | -f], in-place
      std::vector<double> aug(nConstraints * (nConstraints + 1));
      for (int r = 0; r < nConstraints; ++r) {
        for (int col = 0; col < nConstraints; ++col) {
          aug[r * (nConstraints + 1) + col] = W[r * nConstraints + col];
        }
        aug[r * (nConstraints + 1) + nConstraints] = -f[r];
      }
      for (int col = 0; col < nConstraints; ++col) {
        // Find pivot
        int pivot = col;
        for (int r = col + 1; r < nConstraints; ++r) {
          if (std::abs(aug[r * (nConstraints + 1) + col]) >
              std::abs(aug[pivot * (nConstraints + 1) + col])) {
            pivot = r;
          }
        }
        // Swap rows
        if (pivot != col) {
          for (int k = 0; k <= nConstraints; ++k) {
            std::swap(aug[col * (nConstraints + 1) + k],
                      aug[pivot * (nConstraints + 1) + k]);
          }
        }
        const double diag = aug[col * (nConstraints + 1) + col];
        if (std::abs(diag) < detail::kSingularityEps) break;
        for (int r = col + 1; r < nConstraints; ++r) {
          const double factor = aug[r * (nConstraints + 1) + col] / diag;
          for (int k = col; k <= nConstraints; ++k) {
            aug[r * (nConstraints + 1) + k] -=
                factor * aug[col * (nConstraints + 1) + k];
          }
        }
      }
      // Back substitution
      for (int r = nConstraints - 1; r >= 0; --r) {
        double sum = aug[r * (nConstraints + 1) + nConstraints];
        for (int k = r + 1; k < nConstraints; ++k) {
          sum -= aug[r * (nConstraints + 1) + k] * lambda[k];
        }
        const double diag = aug[r * (nConstraints + 1) + r];
        lambda[r] = (std::abs(diag) > detail::kSingularityEps) ? sum / diag : 0.0;
      }
    }

    // ── update particles: delta_eta = V * D^T * lambda ───────────────────
    for (int i = 0; i < nParticles; ++i) {
      double dpt = 0.0, deta = 0.0, dphi = 0.0;
      for (int c = 0; c < nConstraints; ++c) {
        dpt  += var[3 * i + 0] * D[c * nParams + 3 * i + 0] * lambda[c];
        deta += var[3 * i + 1] * D[c * nParams + 3 * i + 1] * lambda[c];
        dphi += var[3 * i + 2] * D[c * nParams + 3 * i + 2] * lambda[c];
      }
      current[i].pt  += dpt;
      current[i].eta += deta;
      current[i].phi += dphi;
      if (current[i].pt < detail::kMinPt) current[i].pt = detail::kMinPt; // keep pT physical
    }

    // ── compute chi2 ─────────────────────────────────────────────────────
    double chi2 = 0.0;
    for (int i = 0; i < nParticles; ++i) {
      const double dpt  = current[i].pt  - particles_[i].pt;
      const double deta = current[i].eta - particles_[i].eta;
      const double dphi = current[i].phi - particles_[i].phi;
      chi2 += dpt  * dpt  / var[3 * i + 0];
      chi2 += deta * deta / var[3 * i + 1];
      chi2 += dphi * dphi / var[3 * i + 2];
    }

    if (std::abs(chi2 - prevChi2) < tolerance) {
      return {current, chi2, iter + 1, true};
    }
    prevChi2 = chi2;
  }

  // Return non-converged result with last chi2
  double chi2 = 0.0;
  for (int i = 0; i < nParticles; ++i) {
    const double dpt  = current[i].pt  - particles_[i].pt;
    const double deta = current[i].eta - particles_[i].eta;
    const double dphi = current[i].phi - particles_[i].phi;
    chi2 += dpt  * dpt  / var[3 * i + 0];
    chi2 += deta * deta / var[3 * i + 1];
    chi2 += dphi * dphi / var[3 * i + 2];
  }
  return {current, chi2, maxIter, false};
}

#endif // KINEMATICFIT_H_INCLUDED
