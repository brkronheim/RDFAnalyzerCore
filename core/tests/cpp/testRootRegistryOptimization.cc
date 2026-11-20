#include <RootRegistryOptimization.h>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RDFHelpers.hxx>
#include <gtest/gtest.h>
#include <cstdlib>
#if RDF_EXPECT_MIMALLOC
#include <mimalloc.h>
#include <TArrayD.h>
#endif
#if RDF_EXPECT_PACKED_READERS
extern "C" unsigned long rdf_root_packed_reader_bindings_v1();
#endif

TEST(RootRegistryOptimization, ReportsLinkedImplementationAndPrunesUnusedBindings) {
  const auto initial = rdf::rootRegistryOptimizationStatus();
#if RDF_EXPECT_ROOT_REGISTRY
  ASSERT_TRUE(initial.linked);
  const char *mode = std::getenv("RDF_ROOT_REGISTRY_OPTIMIZATION");
  const bool expected = !mode || std::string(mode) != "0";
  EXPECT_EQ(initial.enabled, expected);
#else
  EXPECT_FALSE(initial.linked);
#endif
  ROOT::RDataFrame source(10);
  ROOT::RDF::RNode node = source;
  for (int i=0; i<200; ++i)
    node = node.Define("unused_"+std::to_string(i), [i] { return i; });
  node = node.Define("value", [](int i) { return double(i+1); }, {"unused_199"});
  EXPECT_DOUBLE_EQ(*node.Sum<double>("value"), 2000.);
  EXPECT_TRUE(node.HasColumn("unused_0"));
  EXPECT_EQ(node.GetColumnType("unused_0"), "int");
  const auto final = rdf::rootRegistryOptimizationStatus();
  if (final.enabled) {
    EXPECT_GT(final.definitions, initial.definitions);
    EXPECT_GT(final.inputBindingsBefore-initial.inputBindingsBefore,
              final.inputBindingsAfter-initial.inputBindingsAfter);
  }
}

TEST(RootRegistryOptimization, JittedRedefineKeepsCapturedInputsAndNativeVariations) {
  ROOT::RDataFrame source(10);
  auto node = source.Define("x", "double(rdfentry_+1)")
      .Alias("alias", "x").Define("old", "2*alias")
      .Redefine("x", "x+10").Define("fresh", "2*alias");
  auto old = node.Sum<double>("old");
  auto fresh = node.Sum<double>("fresh");
  EXPECT_DOUBLE_EQ(*old, 110.);
  EXPECT_DOUBLE_EQ(*fresh, 310.);
  auto varied = node.Vary("fresh", "ROOT::RVecD{fresh-1,fresh+1}", {"down","up"}, "shape")
      .Define("derived", "fresh*2");
  auto total = varied.Sum<double>("derived");
  auto results = ROOT::RDF::Experimental::VariationsFor(total);
  EXPECT_DOUBLE_EQ(results["nominal"], 620.);
  EXPECT_DOUBLE_EQ(results["shape:down"], 600.);
  EXPECT_DOUBLE_EQ(results["shape:up"], 640.);
}

TEST(RootDefineReaderOptimization, ReusesWithinAnEntryAndRefreshesOnANewLoop) {
#if RDF_EXPECT_PACKED_READERS
  const auto initialBindings = rdf_root_packed_reader_bindings_v1();
#endif
  ROOT::RDataFrame input(4);
  int offset = 0;
  auto graph = input.Define("value", [&offset](ULong64_t entry) {
                        return static_cast<int>(entry) + offset;
                      }, {"rdfentry_"})
                   .Define("left", [](int value) { return 2 * value; }, {"value"})
                   .Define("right", [](int value) { return 3 * value; }, {"value"})
                   .Define("total", [](int left, int right) { return left + right; },
                           {"left", "right"});
  EXPECT_EQ(*graph.Sum<int>("total"), 30);
  offset = 10;
  EXPECT_EQ(*graph.Sum<int>("total"), 230);
#if RDF_EXPECT_PACKED_READERS
  EXPECT_GT(rdf_root_packed_reader_bindings_v1(), initialBindings);
#endif
}

#if RDF_EXPECT_MIMALLOC
TEST(NativeAllocator, HandlesCAndRootLibraryAllocations) {
  void *buffer = std::malloc(1024);
  ASSERT_NE(buffer, nullptr);
  EXPECT_TRUE(mi_is_in_heap_region(buffer));
  std::free(buffer);
  TArrayD values(1024);
  EXPECT_TRUE(mi_is_in_heap_region(values.GetArray()));
}
#endif
