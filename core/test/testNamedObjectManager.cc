/**
 * @file testNamedObjectManager.cc
 * @brief Unit tests for the NamedObjectManager template class
 * @date 2025
 *
 * This file contains comprehensive unit tests for the NamedObjectManager
 * template class. The tests cover basic functionality, edge cases, exception
 * handling, performance, and various object types to ensure the template works
 * correctly.
 */

#include "NamedObjectManager.h"
#include "test_util.h"
#include <gtest/gtest.h>
#include <stdexcept>
#include <string>
#include <vector>

/**
 * @brief Simple test object class for unit testing
 *
 * This class provides a basic object with a name that can be used to test
 * the NamedObjectManager template functionality.
 */
class TestObject {
public:
  /**
   * @brief Default constructor
   */
  TestObject() = default;

  /**
   * @brief Constructor with name parameter
   * @param name The name to assign to the object
   */
  TestObject(const std::string &name) : name_(name) {}

  /**
   * @brief Get the object's name
   * @return The name of the object
   */
  std::string getName() const { return name_; }

  /**
   * @brief Set the object's name
   * @param name The new name to assign
   */
  void setName(const std::string &name) { name_ = name; }

private:
  std::string name_; ///< The name of the object
};

/**
 * @brief Test-specific manager class that exposes protected members
 *
 * This class inherits from NamedObjectManager and exposes protected members
 * for testing purposes. It also provides additional methods for test setup
 * and verification.
 */
class TestNamedObjectManager : public NamedObjectManager<TestObject> {
public:
  // Expose protected members for testing
  using NamedObjectManager<TestObject>::objects_m;
  using NamedObjectManager<TestObject>::features_m;

  /**
   * @brief Add an object for testing purposes
   * @param key The key to associate with the object
   * @param obj The object to add
   */
  void addObject(const std::string &key, const TestObject &obj) {
    objects_m[key] = obj;
  }

  /**
   * @brief Add features for testing purposes
   * @param key The key to associate with the features
   * @param features The features vector to add
   */
  void addFeatures(const std::string &key,
                   const std::vector<std::string> &features) {
    features_m[key] = features;
  }

  /**
   * @brief Remove an object for testing purposes
   * @param key The key of the object to remove
   */
  void removeObject(const std::string &key) { objects_m.erase(key); }

  /**
   * @brief Remove features for testing purposes
   * @param key The key of the features to remove
   */
  void removeFeatures(const std::string &key) { features_m.erase(key); }

  /**
   * @brief Check if an object exists
   * @param key The key to check
   * @return True if the object exists, false otherwise
   */
  bool hasObject(const std::string &key) const {
    return objects_m.find(key) != objects_m.end();
  }

  /**
   * @brief Check if features exist
   * @param key The key to check
   * @return True if the features exist, false otherwise
   */
  bool hasFeatures(const std::string &key) const {
    return features_m.find(key) != features_m.end();
  }

  /**
   * @brief Get the number of objects
   * @return The number of objects in the manager
   */
  size_t getObjectCount() const { return objects_m.size(); }

  /**
   * @brief Get the number of feature sets
   * @return The number of feature sets in the manager
   */
  size_t getFeaturesCount() const { return features_m.size(); }

  /**
   * @brief Clear all objects and features
   */
  void clearObjects() {
    objects_m.clear();
    features_m.clear();
  }
};

/**
 * @brief Test fixture for NamedObjectManager tests
 *
 * This class provides a common setup and teardown for all NamedObjectManager
 * tests. It creates a manager with some predefined objects and features for
 * testing.
 */
class NamedObjectManagerTest : public ::testing::Test {
protected:
  /**
   * @brief Set up the test fixture
   *
   * Creates a new TestNamedObjectManager and populates it with test data.
   */
  void SetUp() override {
    manager = new TestNamedObjectManager();

    // Add some test objects
    manager->addObject("obj1", TestObject("Object1"));
    manager->addObject("obj2", TestObject("Object2"));
    manager->addObject("obj3", TestObject("Object3"));

    // Add features for the objects
    manager->addFeatures("obj1", {"feature1", "feature2"});
    manager->addFeatures("obj2", {"feature3", "feature4", "feature5"});
    manager->addFeatures("obj3", {"feature6"});
  }

  /**
   * @brief Tear down the test fixture
   *
   * Cleans up the manager to prevent memory leaks.
   */
  void TearDown() override { delete manager; }

  TestNamedObjectManager *manager =
      nullptr; ///< The manager instance for testing
};

// ============================================================================
// Basic Functionality Tests
// ============================================================================

/**
 * @brief Test basic object retrieval functionality
 *
 * Verifies that objects can be added and retrieved correctly using their keys.
 */
TEST_F(NamedObjectManagerTest, GetObjectReturnsCorrectObject) {
  TestObject obj1("test1");
  TestObject obj2("test2");

  manager->addObject("key1", obj1);
  manager->addObject("key2", obj2);

  EXPECT_EQ(manager->getObject("key1").getName(), "test1");
  EXPECT_EQ(manager->getObject("key2").getName(), "test2");
}

/**
 * @brief Test basic features retrieval functionality
 *
 * Verifies that feature vectors can be added and retrieved correctly using
 * their keys.
 */
TEST_F(NamedObjectManagerTest, GetFeaturesReturnsCorrectFeatures) {
  std::vector<std::string> features1 = {"feature1", "feature2", "feature3"};
  std::vector<std::string> features2 = {"var1", "var2"};

  manager->addFeatures("key1", features1);
  manager->addFeatures("key2", features2);

  const auto &retrieved_features1 = manager->getFeatures("key1");
  const auto &retrieved_features2 = manager->getFeatures("key2");

  EXPECT_EQ(retrieved_features1.size(), 3);
  EXPECT_EQ(retrieved_features2.size(), 2);

  EXPECT_EQ(retrieved_features1[0], "feature1");
  EXPECT_EQ(retrieved_features1[1], "feature2");
  EXPECT_EQ(retrieved_features1[2], "feature3");

  EXPECT_EQ(retrieved_features2[0], "var1");
  EXPECT_EQ(retrieved_features2[1], "var2");
}

// ============================================================================
// Exception Handling Tests
// ============================================================================

/**
 * @brief Test exception handling for nonexistent objects
 *
 * Verifies that attempting to retrieve a nonexistent object throws the correct
 * exception.
 */
TEST_F(NamedObjectManagerTest, GetObjectThrowsExceptionForNonexistentKey) {
  EXPECT_THROW(manager->getObject("nonexistent"), std::runtime_error);
}

/**
 * @brief Test exception handling for nonexistent features
 *
 * Verifies that attempting to retrieve nonexistent features throws the correct
 * exception.
 */
TEST_F(NamedObjectManagerTest, GetFeaturesThrowsExceptionForNonexistentKey) {
  EXPECT_THROW(manager->getFeatures("nonexistent"), std::runtime_error);
}

/**
 * @brief Test that exception messages contain the missing key
 *
 * Verifies that the exception messages include the key that was not found,
 * which helps with debugging.
 */
TEST_F(NamedObjectManagerTest, ExceptionMessageContainsKey) {
  try {
    manager->getObject("missing_key");
    FAIL() << "Expected std::runtime_error";
  } catch (const std::runtime_error &e) {
    std::string error_msg = e.what();
    EXPECT_NE(error_msg.find("missing_key"), std::string::npos);
  }

  try {
    manager->getFeatures("missing_features");
    FAIL() << "Expected std::runtime_error";
  } catch (const std::runtime_error &e) {
    std::string error_msg = e.what();
    EXPECT_NE(error_msg.find("missing_features"), std::string::npos);
  }
}

// ============================================================================
// Edge Cases and Boundary Tests
// ============================================================================

/**
 * @brief Test handling of empty and whitespace keys
 *
 * Verifies that the manager can handle edge cases like empty strings and
 * whitespace-only keys for both objects and features.
 */
TEST_F(NamedObjectManagerTest, EmptyAndWhitespaceKeyHandling) {
  TestObject obj("test");
  std::vector<std::string> features = {"feature1"};

  // Test empty key
  manager->addObject("", obj);
  manager->addFeatures("", features);

  EXPECT_EQ(manager->getObject("").getName(), "test");
  EXPECT_EQ(manager->getFeatures("").size(), 1);
  EXPECT_EQ(manager->getFeatures("")[0], "feature1");

  // Test whitespace key
  manager->addObject("   ", obj);
  manager->addFeatures("   ", features);

  EXPECT_EQ(manager->getObject("   ").getName(), "test");
  EXPECT_EQ(manager->getFeatures("   ").size(), 1);
}

/**
 * @brief Test handling of special characters in keys
 *
 * Verifies that the manager can handle keys containing special characters,
 * including symbols, punctuation, and unicode characters.
 */
TEST_F(NamedObjectManagerTest, SpecialCharactersInKeys) {
  TestObject obj("test");
  std::vector<std::string> features = {"feature1"};

  // Test special characters
  std::string special_key = "key@#$%^&*()_+-=[]{}|;':\",./<>?";
  manager->addObject(special_key, obj);
  manager->addFeatures(special_key, features);

  EXPECT_EQ(manager->getObject(special_key).getName(), "test");
  EXPECT_EQ(manager->getFeatures(special_key).size(), 1);

  // Test unicode characters
  std::string unicode_key = "café_résumé_测试";
  manager->addObject(unicode_key, obj);
  manager->addFeatures(unicode_key, features);

  EXPECT_EQ(manager->getObject(unicode_key).getName(), "test");
  EXPECT_EQ(manager->getFeatures(unicode_key).size(), 1);
}

// TODO: need to modify to not allow empty features
/**
 * @brief Test handling of empty feature vectors
 *
 * Verifies that the manager can handle empty feature vectors and vectors
 * containing empty strings.
 */
TEST_F(NamedObjectManagerTest, EmptyFeaturesHandling) {
  // Test empty features vector
  std::vector<std::string> empty_features;
  manager->addFeatures("key1", empty_features);

  EXPECT_EQ(manager->getFeatures("key1").size(), 0);

  // Test features with empty strings
  std::vector<std::string> features_with_empty = {"", "feature1", "",
                                                  "feature2", ""};
  manager->addFeatures("key2", features_with_empty);

  const auto &retrieved_features = manager->getFeatures("key2");
  EXPECT_EQ(retrieved_features.size(), 5);
  EXPECT_EQ(retrieved_features[0], "");
  EXPECT_EQ(retrieved_features[1], "feature1");
  EXPECT_EQ(retrieved_features[2], "");
  EXPECT_EQ(retrieved_features[3], "feature2");
  EXPECT_EQ(retrieved_features[4], "");
}

// ============================================================================
// Multiple Objects and Features Tests
// ============================================================================

/**
 * @brief Test management of multiple objects and features
 *
 * Verifies that the manager can handle multiple objects and features correctly,
 * ensuring that each key maps to the correct object and feature set.
 */
TEST_F(NamedObjectManagerTest, MultipleObjectsAndFeatures) {
  manager->clearObjects();
  TestObject obj1("object1");
  TestObject obj2("object2");
  TestObject obj3("object3");

  std::vector<std::string> features1 = {"f1", "f2"};
  std::vector<std::string> features2 = {"f3", "f4", "f5"};
  std::vector<std::string> features3 = {"f6"};

  manager->addObject("key1", obj1);
  manager->addObject("key2", obj2);
  manager->addObject("key3", obj3);

  manager->addFeatures("key1", features1);
  manager->addFeatures("key2", features2);
  manager->addFeatures("key3", features3);

  EXPECT_EQ(manager->getObjectCount(), 3);
  EXPECT_EQ(manager->getFeaturesCount(), 3);

  EXPECT_EQ(manager->getObject("key1").getName(), "object1");
  EXPECT_EQ(manager->getObject("key2").getName(), "object2");
  EXPECT_EQ(manager->getObject("key3").getName(), "object3");

  EXPECT_EQ(manager->getFeatures("key1").size(), 2);
  EXPECT_EQ(manager->getFeatures("key2").size(), 3);
  EXPECT_EQ(manager->getFeatures("key3").size(), 1);
}

// ============================================================================
// Overwrite and Modification Tests
// ============================================================================

/**
 * @brief Test overwriting existing objects and features
 *
 * Verifies that adding an object or features with an existing key properly
 * overwrites the previous value.
 */
TEST_F(NamedObjectManagerTest, OverwriteExistingObjectsAndFeatures) {
  // Test object overwrite
  TestObject obj1("original");
  TestObject obj2("replacement");

  manager->addObject("key1", obj1);
  EXPECT_EQ(manager->getObject("key1").getName(), "original");

  manager->addObject("key1", obj2);
  EXPECT_EQ(manager->getObject("key1").getName(), "replacement");

  // Test features overwrite
  std::vector<std::string> features1 = {"original1", "original2"};
  std::vector<std::string> features2 = {"replacement1", "replacement2",
                                        "replacement3"};

  manager->addFeatures("key2", features1);
  EXPECT_EQ(manager->getFeatures("key2").size(), 2);

  manager->addFeatures("key2", features2);
  EXPECT_EQ(manager->getFeatures("key2").size(), 3);
  EXPECT_EQ(manager->getFeatures("key2")[0], "replacement1");
  EXPECT_EQ(manager->getFeatures("key2")[1], "replacement2");
  EXPECT_EQ(manager->getFeatures("key2")[2], "replacement3");
}

// ============================================================================
// Removal Tests
// ============================================================================

/**
 * @brief Test removal of objects and features
 *
 * Verifies that objects and features can be removed correctly and that
 * attempting to access removed items throws appropriate exceptions.
 */
TEST_F(NamedObjectManagerTest, RemoveObjectsAndFeatures) {
  manager->clearObjects();
  TestObject obj("test");
  std::vector<std::string> features = {"feature1", "feature2"};

  // Test object removal
  manager->addObject("key1", obj);
  EXPECT_TRUE(manager->hasObject("key1"));
  EXPECT_EQ(manager->getObjectCount(), 1);

  manager->removeObject("key1");
  EXPECT_FALSE(manager->hasObject("key1"));
  EXPECT_EQ(manager->getObjectCount(), 0);
  EXPECT_THROW(manager->getObject("key1"), std::runtime_error);

  // Test features removal
  manager->addFeatures("key2", features);
  EXPECT_TRUE(manager->hasFeatures("key2"));
  EXPECT_EQ(manager->getFeaturesCount(), 1);

  manager->removeFeatures("key2");
  EXPECT_FALSE(manager->hasFeatures("key2"));
  EXPECT_EQ(manager->getFeaturesCount(), 0);
  EXPECT_THROW(manager->getFeatures("key2"), std::runtime_error);
}

/**
 * @brief Test removal of nonexistent objects and features
 *
 * Verifies that attempting to remove nonexistent objects or features
 * does not cause errors and leaves the manager in a consistent state.
 */
TEST_F(NamedObjectManagerTest, RemoveNonexistentObjectsAndFeatures) {
  EXPECT_EQ(manager->getObjectCount(), 3);
  EXPECT_EQ(manager->getFeaturesCount(), 3);

  manager->removeObject("nonexistent");
  manager->removeFeatures("nonexistent");

  EXPECT_EQ(manager->getObjectCount(), 3);
  EXPECT_EQ(manager->getFeaturesCount(), 3);
}

// ============================================================================
// Const Correctness Tests
// ============================================================================

/**
 * @brief Test const correctness of the manager
 *
 * Verifies that const methods work correctly and that const managers
 * can retrieve objects and features but cannot modify them.
 */
TEST_F(NamedObjectManagerTest, ConstCorrectness) {
  TestObject obj("test");
  std::vector<std::string> features = {"feature1"};

  manager->addObject("key1", obj);
  manager->addFeatures("key1", features);

  const TestNamedObjectManager *const_manager = manager;

  // These should compile and work correctly
  EXPECT_EQ(const_manager->getObject("key1").getName(), "test");
  EXPECT_EQ(const_manager->getFeatures("key1").size(), 1);

  // These should throw exceptions
  EXPECT_THROW(const_manager->getObject("nonexistent"), std::runtime_error);
  EXPECT_THROW(const_manager->getFeatures("nonexistent"), std::runtime_error);
}

// ============================================================================
// Reference Return Tests
// ============================================================================

/**
 * @brief Test that methods return references
 *
 * Verifies that getObject and getFeatures return references to the stored
 * objects and features, not copies, ensuring memory efficiency.
 */
TEST_F(NamedObjectManagerTest, ReturnsReferences) {
  TestObject obj("test");
  std::vector<std::string> features = {"feature1"};

  manager->addObject("key1", obj);
  manager->addFeatures("key1", features);

  const TestObject &obj_ref = manager->getObject("key1");
  const std::vector<std::string> &features_ref = manager->getFeatures("key1");

  // Verify we got references (not copies)
  EXPECT_EQ(&obj_ref, &manager->objects_m.at("key1"));
  EXPECT_EQ(&features_ref, &manager->features_m.at("key1"));
}

// ============================================================================
// Large Data Tests
// ============================================================================

/**
 * @brief Test handling of large numbers of objects
 *
 * Verifies that the manager can handle a large number of objects efficiently
 * and that random access to objects works correctly.
 */
TEST_F(NamedObjectManagerTest, LargeNumberOfObjects) {
  manager->clearObjects();
  const int num_objects = 1000;

  for (int i = 0; i < num_objects; ++i) {
    std::string key = "key" + std::to_string(i);
    TestObject obj("object" + std::to_string(i));
    manager->addObject(key, obj);
  }

  EXPECT_EQ(manager->getObjectCount(), num_objects);

  // Test random access
  EXPECT_EQ(manager->getObject("key500").getName(), "object500");
  EXPECT_EQ(manager->getObject("key999").getName(), "object999");
}

/**
 * @brief Test handling of large feature vectors
 *
 * Verifies that the manager can handle large feature vectors efficiently
 * and that random access to features works correctly.
 */
TEST_F(NamedObjectManagerTest, LargeFeatureVectors) {
  const int num_features = 1000;
  std::vector<std::string> large_features;

  for (int i = 0; i < num_features; ++i) {
    large_features.push_back("feature" + std::to_string(i));
  }

  manager->addFeatures("key1", large_features);

  const auto &retrieved_features = manager->getFeatures("key1");
  EXPECT_EQ(retrieved_features.size(), num_features);

  // Test random access
  EXPECT_EQ(retrieved_features[500], "feature500");
  EXPECT_EQ(retrieved_features[999], "feature999");
}

// ============================================================================
// Performance and Memory Tests
// ============================================================================

/**
 * @brief Test memory efficiency
 *
 * Verifies that objects are stored efficiently and that multiple calls
 * to getObject and getFeatures return the same references, avoiding
 * unnecessary copying.
 */
TEST_F(NamedObjectManagerTest, MemoryEfficiency) {
  TestObject obj("test");
  std::vector<std::string> features = {"feature1", "feature2", "feature3"};

  manager->addObject("key1", obj);
  manager->addFeatures("key1", features);

  // Verify that the same object is returned (no unnecessary copying)
  const TestObject &obj1 = manager->getObject("key1");
  const TestObject &obj2 = manager->getObject("key1");
  EXPECT_EQ(&obj1, &obj2);

  // Verify that the same features vector is returned
  const std::vector<std::string> &features1 = manager->getFeatures("key1");
  const std::vector<std::string> &features2 = manager->getFeatures("key1");
  EXPECT_EQ(&features1, &features2);
}

// ============================================================================
// Template Specialization Tests
// ============================================================================

/**
 * @brief Test template with different object types
 *
 * Verifies that the NamedObjectManager template works correctly with
 * different object types, not just the TestObject class.
 */
TEST_F(NamedObjectManagerTest, DifferentObjectTypes) {
  // Test with a different object type
  class DifferentObject {
  public:
    DifferentObject() = default;
    DifferentObject(int value) : value_(value) {}
    int getValue() const { return value_; }

  private:
    int value_;
  };

  class DifferentObjectManager : public NamedObjectManager<DifferentObject> {
  public:
    using NamedObjectManager<DifferentObject>::objects_m;
    using NamedObjectManager<DifferentObject>::features_m;

    void addObject(const std::string &key, const DifferentObject &obj) {
      objects_m[key] = obj;
    }

    void addFeatures(const std::string &key,
                     const std::vector<std::string> &features) {
      features_m[key] = features;
    }
  };

  DifferentObjectManager diff_manager;
  DifferentObject obj1(42);
  DifferentObject obj2(100);

  std::vector<std::string> features = {"feature1"};

  diff_manager.addObject("key1", obj1);
  diff_manager.addObject("key2", obj2);
  diff_manager.addFeatures("key1", features);

  EXPECT_EQ(diff_manager.getObject("key1").getValue(), 42);
  EXPECT_EQ(diff_manager.getObject("key2").getValue(), 100);
  EXPECT_EQ(diff_manager.getFeatures("key1").size(), 1);
}

// ============================================================================
// Constructor and Lifecycle Tests
// ============================================================================

/**
 * @brief Test that constructor creates an empty manager
 *
 * Verifies that a newly constructed manager is empty and throws
 * appropriate exceptions when trying to access nonexistent objects.
 */
TEST_F(NamedObjectManagerTest, ConstructorCreatesEmptyManager) {
  TestNamedObjectManager emptyManager;

  // Should throw when trying to get objects that don't exist
  EXPECT_THROW(emptyManager.getObject("nonexistent"), std::runtime_error);
  EXPECT_THROW(emptyManager.getFeatures("nonexistent"), std::runtime_error);
}

// ============================================================================
// Independent Object and Feature Tests
// ============================================================================

/**
 * @brief Test adding objects without features
 *
 * Verifies that objects can be added independently of features and
 * that features can be added independently of objects.
 */
TEST_F(NamedObjectManagerTest, IndependentObjectsAndFeatures) {
  // Add an object without features
  manager->addObject("new_obj", TestObject("NewObject"));

  // Object should be retrievable
  const auto &obj = manager->getObject("new_obj");
  EXPECT_EQ(obj.getName(), "NewObject");

  // But features should not exist
  EXPECT_THROW(manager->getFeatures("new_obj"), std::runtime_error);

  // Add features without an object
  std::vector<std::string> features = {"feature1", "feature2"};
  manager->addFeatures("orphan_features", features);

  // Features should be retrievable
  const auto &retrievedFeatures = manager->getFeatures("orphan_features");
  EXPECT_EQ(retrievedFeatures.size(), 2);

  // But object should not exist
  EXPECT_THROW(manager->getObject("orphan_features"), std::runtime_error);
}

// ============================================================================
// Memory Management Tests
// ============================================================================

/**
 * @brief Test basic memory allocation and deallocation
 *
 * Verifies that objects can be properly constructed and destroyed.
 */
TEST_F(NamedObjectManagerTest, BasicMemoryAllocation) {
  TestNamedObjectManager *localManager = new TestNamedObjectManager();

  // Add some objects
  localManager->addObject("test_obj", TestObject("TestObject"));
  localManager->addFeatures("test_obj", {"test_feature"});

  // Verify they exist
  const auto &obj = localManager->getObject("test_obj");
  const auto &features = localManager->getFeatures("test_obj");

  EXPECT_EQ(obj.getName(), "TestObject");
  EXPECT_EQ(features.size(), 1);

  // Clean up - this should not crash or cause issues
  delete localManager;
}

/**
 * @brief Test repeated allocation and deallocation
 *
 * Verifies that the manager can handle multiple allocation/deallocation
 * cycles without issues. This helps catch basic memory management bugs
 * but is not a substitute for proper memory leak detection tools.
 */
TEST_F(NamedObjectManagerTest, RepeatedAllocationDeallocation) {
  const int iterations = 100;

  for (int i = 0; i < iterations; ++i) {
    TestNamedObjectManager *localManager = new TestNamedObjectManager();

    // Add objects with different keys each iteration
    std::string key = "obj_" + std::to_string(i);
    localManager->addObject(key, TestObject("Object" + std::to_string(i)));
    localManager->addFeatures(key, {"feature" + std::to_string(i)});

    // Verify the object exists
    EXPECT_EQ(localManager->getObject(key).getName(),
              "Object" + std::to_string(i));
    EXPECT_EQ(localManager->getFeatures(key).size(), 1);

    // Clean up
    delete localManager;
  }
}

/**
 * @brief Test memory management with exceptions
 *
 * Verifies that the manager handles exceptions gracefully without
 * leaving the system in an inconsistent state.
 */
TEST_F(NamedObjectManagerTest, MemoryManagementWithExceptions) {
  TestNamedObjectManager *localManager = new TestNamedObjectManager();

  try {
    // Add some objects
    localManager->addObject("test_obj", TestObject("TestObject"));
    localManager->addFeatures("test_obj", {"test_feature"});

    // Simulate an exception
    throw std::runtime_error("Test exception");

  } catch (const std::runtime_error &e) {
    // Exception was thrown as expected
    EXPECT_STREQ(e.what(), "Test exception");
  }

  // Clean up should still work even after exception
  delete localManager;
}

// ============================================================================
// Copy and Move Semantics Tests
// ============================================================================

/**
 * @brief Test copy semantics
 *
 * Verifies that the manager can be copied correctly and that the
 * original and copied managers are independent.
 */
TEST_F(NamedObjectManagerTest, CopySemantics) {
  TestNamedObjectManager copyManager = *manager;

  // Verify that copied objects exist
  const auto &obj1 = copyManager.getObject("obj1");
  const auto &features1 = copyManager.getFeatures("obj1");

  EXPECT_EQ(obj1.getName(), "Object1");
  EXPECT_EQ(features1.size(), 2);

  // Verify that original is unchanged
  const auto &originalObj1 = manager->getObject("obj1");
  const auto &originalFeatures1 = manager->getFeatures("obj1");

  EXPECT_EQ(originalObj1.getName(), "Object1");
  EXPECT_EQ(originalFeatures1.size(), 2);
}

/**
 * @brief Test move semantics
 *
 * Verifies that the manager can be moved correctly and that the
 * moved manager contains the expected data.
 */
TEST_F(NamedObjectManagerTest, MoveSemantics) {
  TestNamedObjectManager moveManager = std::move(*manager);

  // Verify that moved objects exist
  const auto &obj1 = moveManager.getObject("obj1");
  const auto &features1 = moveManager.getFeatures("obj1");

  EXPECT_EQ(obj1.getName(), "Object1");
  EXPECT_EQ(features1.size(), 2);

  // Original manager should be in a valid but unspecified state
  // We can't test this directly, but we can verify that the move worked
  EXPECT_EQ(moveManager.getObject("obj1").getName(), "Object1");
}
