#include <CartesianBinning.h>

#include <gtest/gtest.h>

#include <limits>
#include <optional>
#include <vector>

TEST(CartesianBinningTest, FlattensAndUnflattensWithLastAxisFastest) {
  const RDFAnalyzer::CartesianBinning binning({3, 2});

  EXPECT_EQ(binning.numberOfBins(), 6U);
  EXPECT_EQ(binning.flatten({0, 0}), 0U);
  EXPECT_EQ(binning.flatten({0, 1}), 1U);
  EXPECT_EQ(binning.flatten({1, 0}), 2U);
  EXPECT_EQ(binning.flatten({2, 1}), 5U);
  EXPECT_EQ(binning.unflatten(4), (std::vector<std::size_t>{2, 0}));
}

TEST(CartesianBinningTest, ReturnsNulloptForInvalidCoordinates) {
  const RDFAnalyzer::CartesianBinning binning({2, 3});

  EXPECT_FALSE(binning.flattenOptional(
                         {std::size_t{0}, std::nullopt})
                   .has_value());
  EXPECT_FALSE(binning.flattenOptional(
                         {std::size_t{2}, std::size_t{0}})
                   .has_value());
  EXPECT_FALSE(binning.flattenOptional({std::size_t{0}}).has_value());
}

TEST(CartesianBinningTest, NumericAxisUsesHalfOpenBinsAndOpenFinalBin) {
  const RDFAnalyzer::NumericBinningAxis axis({0.0, 40.0, 100.0});
  const RDFAnalyzer::NumericBinningAxis floatAxis(
      std::vector<float>{0.0F, 40.0F, 100.0F});

  EXPECT_EQ(axis.numberOfBins(), 3U);
  EXPECT_EQ(floatAxis.findBin(40.0), std::optional<std::size_t>{1});
  EXPECT_FALSE(axis.findBin(-0.1).has_value());
  EXPECT_EQ(axis.findBin(0.0), std::optional<std::size_t>{0});
  EXPECT_EQ(axis.findBin(40.0), std::optional<std::size_t>{1});
  EXPECT_EQ(axis.findBin(1000.0), std::optional<std::size_t>{2});
  EXPECT_FALSE(axis.findBin(std::numeric_limits<double>::quiet_NaN()).has_value());
}

TEST(CartesianBinningTest, RejectsMalformedAxes) {
  EXPECT_THROW(RDFAnalyzer::CartesianBinning({}), std::invalid_argument);
  EXPECT_THROW(RDFAnalyzer::CartesianBinning({2, 0}), std::invalid_argument);
  EXPECT_THROW(RDFAnalyzer::NumericBinningAxis({0.0}), std::invalid_argument);
  EXPECT_THROW(RDFAnalyzer::NumericBinningAxis({0.0, 0.0}), std::invalid_argument);
}