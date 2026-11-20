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

TEST(CartesianBinningTest, OptionalCoordinatesAgreeWithEveryValidBin) {
  const RDFAnalyzer::CartesianBinning layout({2, 3, 4});
  for (std::size_t bin = 0; bin < layout.numberOfBins(); ++bin) {
    const auto coords = layout.unflatten(bin);
    EXPECT_EQ(layout.flattenOptional({coords[0], coords[1], coords[2]}), bin);
  }
}

TEST(CartesianBinningTest, FixedCategoriesCheckRepresentationBoundaries) {
  using RDFAnalyzer::flattenCategories;
  using RDFAnalyzer::unflattenCategories;
  const auto max = std::numeric_limits<std::size_t>::max();
  const auto intMax = std::numeric_limits<int>::max();
  EXPECT_EQ(flattenCategories<2>({2, max}, {1, 0}), max);
  EXPECT_THROW(flattenCategories<2>({2, max}, {1, 1}), std::overflow_error);
  EXPECT_THROW(flattenCategories<2>({3, max}, {2, 0}), std::overflow_error);
  EXPECT_THROW(flattenCategories<1>({2}, {1}, max), std::overflow_error);
  EXPECT_EQ(unflattenCategories<1>({2}, max, max - 1)[0], 1);
  EXPECT_EQ(unflattenCategories<1>({max}, intMax)[0], intMax);
  EXPECT_THROW(unflattenCategories<1>({max}, std::size_t(intMax) + 1),
               std::overflow_error);
}
