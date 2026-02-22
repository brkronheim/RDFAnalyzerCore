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
 * @brief Invariant mass constraint for two or three particles.
 *
 * When @p idx3 >= 0 the constraint is on the three-body invariant mass
 *   m(p_idx1, p_idx2, p_idx3) = @p targetMass.
 * When @p idx3 == -1 the constraint is the standard two-body mass
 *   m(p_idx1, p_idx2) = @p targetMass.
 *
 * Three-body constraints are essential for top-quark decays:
 *   t → b W → b l ν  ⟹  m(b, l, ν) = 173.3 GeV
 * or fully hadronic:
 *   t → b W → b j j  ⟹  m(b, j1, j2) = 173.3 GeV
 */
struct MassConstraint {
  int idx1;          ///< Index of the first  particle
  int idx2;          ///< Index of the second particle
  int idx3 = -1;     ///< Index of the third  particle; -1 means two-body constraint
  double targetMass; ///< Target invariant mass [GeV]
};

/**
 * @brief Transverse-momentum (size) constraint on a single particle.
 *
 * Constrains the fitted pT of particle @p idx to equal @p targetPt.
 * Setting @p targetPt = 0 effectively removes the particle's contribution
 * to hadronic MET in events where no genuine MET is expected (e.g. fully
 * hadronic decays).
 *
 * Example: constrain MET to 0 in a Z → jj selection:
 * @code
 *   fitter.addPtConstraint(metIdx, 0.0);
 * @endcode
 */
struct PtConstraint {
  int    idx;       ///< Index of the particle whose pT is constrained
  double targetPt;  ///< Target transverse momentum [GeV]
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
    constraints_.push_back({idx1, idx2, -1, targetMass});
  }

  /**
   * @brief Add a three-body invariant mass constraint.
   *
   * Constrains m(p_idx1, p_idx2, p_idx3) = targetMass.  Typical use case is
   * the top-quark decay: t → b l ν (or t → b j j), where you want to
   * constrain the invariant mass of all three decay products to the top mass.
   * @param idx1       Index of the first  particle.
   * @param idx2       Index of the second particle.
   * @param idx3       Index of the third  particle.
   * @param targetMass Target invariant mass [GeV].
   */
  void addThreeBodyMassConstraint(int idx1, int idx2, int idx3,
                                  double targetMass) {
    constraints_.push_back({idx1, idx2, idx3, targetMass});
  }

  /**
   * @brief Add a transverse-momentum constraint on a single particle.
   *
   * Constrains the fitted pT of particle @p idx to equal @p targetPt.
   * Use @p targetPt = 0.0 to suppress MET in events where no genuine
   * missing energy is expected (e.g. fully hadronic decays).
   * @param idx      Index of the particle.
   * @param targetPt Target pT [GeV].
   */
  void addPtConstraint(int idx, double targetPt) {
    ptConstraints_.push_back({idx, targetPt});
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
  std::vector<PtConstraint>   ptConstraints_;

  // ── helpers ────────────────────────────────────────────────────────────────────────────

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

  /// Compute (m123)^2 for three particles in their current state.
  static double invMass3(const KinFitParticle &p1, const KinFitParticle &p2,
                          const KinFitParticle &p3) {
    auto [E1, px1, py1, pz1] = fourMomentum(p1.pt, p1.eta, p1.phi, p1.mass);
    auto [E2, px2, py2, pz2] = fourMomentum(p2.pt, p2.eta, p2.phi, p2.mass);
    auto [E3, px3, py3, pz3] = fourMomentum(p3.pt, p3.eta, p3.phi, p3.mass);
    const double dE  = E1  + E2  + E3;
    const double dpx = px1 + px2 + px3;
    const double dpy = py1 + py2 + py3;
    const double dpz = pz1 + pz2 + pz3;
    return dE * dE - dpx * dpx - dpy * dpy - dpz * dpz;
  }

  /**
   * @brief Gradient of m_N^2 w.r.t. (pT, eta, phi) of particle @p pk, given
   *        the total system 4-momentum (sumE, sumPx, sumPy, sumPz) which
   *        already includes @p pk.
   *
   * The invariant mass squared is m_N^2 = (sum E)^2 - |sum p|^2, so
   *   d(m_N^2)/dx = 2 * [(sum E) * dE_k/dx - (sum p) \u00b7 d(p_k)/dx].
   * This formula is identical for two-body and three-body (or N-body)
   * constraints \u2014 only the sums differ.
   *
   * @return {d(m_N^2)/dpT, d(m_N^2)/deta, d(m_N^2)/dphi}
   */
  static std::array<double, 3> gradMassN(const KinFitParticle &pk,
                                          double sumE,  double sumPx,
                                          double sumPy, double sumPz) {
    const double ch = std::cosh(pk.eta);
    const double sh = std::sinh(pk.eta);
    auto [Ek, pxk, pyk, pzk] = fourMomentum(pk.pt, pk.eta, pk.phi, pk.mass);
    (void)pxk; (void)pyk; (void)pzk;

    const double dEk_dpT  = (Ek > detail::kMinEnergy) ? pk.pt * ch * ch / Ek : 0.0;
    const double dEk_deta = (Ek > detail::kMinEnergy) ? pk.pt * pk.pt * sh * ch / Ek : 0.0;

    const double d_dpT  = 2.0 * (sumE * dEk_dpT
                                  - sumPx * std::cos(pk.phi)
                                  - sumPy * std::sin(pk.phi)
                                  - sumPz * sh);
    const double d_deta = 2.0 * (sumE * dEk_deta
                                  - sumPz * pk.pt * ch);
    const double d_dphi = 2.0 * (sumPx * pk.pt * std::sin(pk.phi)
                                  - sumPy * pk.pt * std::cos(pk.phi));
    return {d_dpT, d_deta, d_dphi};
  }

  /**
   * @brief Gradient of (m12)^2 w.r.t. (pT, eta, phi) of particle @p p1.
   * @return {d(m12^2)/dpT1, d(m12^2)/deta1, d(m12^2)/dphi1}
   */
  static std::array<double, 3> gradMass2(const KinFitParticle &p1,
                                          const KinFitParticle &p2) {
    auto [E1, px1, py1, pz1] = fourMomentum(p1.pt, p1.eta, p1.phi, p1.mass);
    auto [E2, px2, py2, pz2] = fourMomentum(p2.pt, p2.eta, p2.phi, p2.mass);
    return gradMassN(p1, E1 + E2, px1 + px2, py1 + py2, pz1 + pz2);
  }
}

// ── KinematicFit::fit implementation (header-only) ────────────────────────

inline KinFitResult KinematicFit::fit(int maxIter, double tolerance) const {
  const int nParticles    = static_cast<int>(particles_.size());
  const int nMassConstr   = static_cast<int>(constraints_.size());
  const int nPtConstr     = static_cast<int>(ptConstraints_.size());
  const int nConstraints  = nMassConstr + nPtConstr;
  const int nParams       = nParticles * 3; // (pT, eta, phi) per particle

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

    // Mass constraints (two-body and three-body)
    for (int c = 0; c < nMassConstr; ++c) {
      const auto &con = constraints_[c];
      const auto &p1  = current[con.idx1];
      const auto &p2  = current[con.idx2];

      if (con.idx3 < 0) {
        // ── Two-body mass constraint ──────────────────────────────────────
        f[c] = invMass2(p1, p2) - con.targetMass * con.targetMass;

        auto [E1,px1,py1,pz1] = fourMomentum(p1.pt,p1.eta,p1.phi,p1.mass);
        auto [E2,px2,py2,pz2] = fourMomentum(p2.pt,p2.eta,p2.phi,p2.mass);
        const double sE  = E1+E2, sPx = px1+px2, sPy = py1+py2, sPz = pz1+pz2;
        auto g1 = gradMassN(p1, sE, sPx, sPy, sPz);
        auto g2 = gradMassN(p2, sE, sPx, sPy, sPz);

        const int i1 = con.idx1 * 3, i2 = con.idx2 * 3;
        D[c * nParams + i1 + 0] = g1[0]; D[c * nParams + i1 + 1] = g1[1]; D[c * nParams + i1 + 2] = g1[2];
        D[c * nParams + i2 + 0] = g2[0]; D[c * nParams + i2 + 1] = g2[1]; D[c * nParams + i2 + 2] = g2[2];
      } else {
        // ── Three-body mass constraint (e.g. top → b l ν) ────────────────
        const auto &p3 = current[con.idx3];
        f[c] = invMass3(p1, p2, p3) - con.targetMass * con.targetMass;

        auto [E1,px1,py1,pz1] = fourMomentum(p1.pt,p1.eta,p1.phi,p1.mass);
        auto [E2,px2,py2,pz2] = fourMomentum(p2.pt,p2.eta,p2.phi,p2.mass);
        auto [E3,px3,py3,pz3] = fourMomentum(p3.pt,p3.eta,p3.phi,p3.mass);
        const double sE  = E1+E2+E3, sPx = px1+px2+px3;
        const double sPy = py1+py2+py3, sPz = pz1+pz2+pz3;
        auto g1 = gradMassN(p1, sE, sPx, sPy, sPz);
        auto g2 = gradMassN(p2, sE, sPx, sPy, sPz);
        auto g3 = gradMassN(p3, sE, sPx, sPy, sPz);

        const int i1 = con.idx1*3, i2 = con.idx2*3, i3 = con.idx3*3;
        D[c*nParams+i1+0]=g1[0]; D[c*nParams+i1+1]=g1[1]; D[c*nParams+i1+2]=g1[2];
        D[c*nParams+i2+0]=g2[0]; D[c*nParams+i2+1]=g2[1]; D[c*nParams+i2+2]=g2[2];
        D[c*nParams+i3+0]=g3[0]; D[c*nParams+i3+1]=g3[1]; D[c*nParams+i3+2]=g3[2];
      }
    }

    // pT constraints (linear: f = pT_i - target, d f/d pT_i = 1)
    for (int k = 0; k < nPtConstr; ++k) {
      const int c  = nMassConstr + k;
      const auto &pc = ptConstraints_[k];
      f[c] = current[pc.idx].pt - pc.targetPt;
      D[c * nParams + pc.idx * 3 + 0] = 1.0; // d f / d pT = 1
      // D[c * nParams + pc.idx * 3 + 1] = 0 (d f / d eta = 0, already zero)
      // D[c * nParams + pc.idx * 3 + 2] = 0 (d f / d phi = 0, already zero)
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
