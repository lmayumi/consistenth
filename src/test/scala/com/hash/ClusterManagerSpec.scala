package com.hash

import org.scalatest._

class ClusterManagerSpec extends WordSpec with Matchers {

  case class StringKey(key: String) extends Key {
    override def toString = key
  }
  case class StringValue(value: String) extends Value {
    override def toString = value
  }

  val k1 = new Key {
    override def toString = "K1" // mapped to a range 1 - 15
  }
  val k2 = new Key {
    override def toString = "K10"// mapped to a range 31 - 45
  }
  val v = new Value {}

  "ClusterManager" should {
    "Insert keys to the cluster" in {
      val manager = new ClusterManager

      assert(manager.put(k1, v) == 15)
      assert(manager.put(k2, v) == 45)
    }

    "Remap keys within range when new node is added to a cluster" in {
      val manager = new ClusterManager

      assert(manager.put(k1, v) == 15)
      assert(manager.put(k2, v) == 45)

      manager.addNodeAt(40)

      assert(manager.put(k1, v) == 15)
      assert(manager.put(k2, v) == 40)
    }

    "Remap keys within range when new node is deleted from a cluster" in {
      val manager = new ClusterManager

      assert(manager.put(k1, v) == 15)
      assert(manager.put(k2, v) == 45)

      manager.removeNodeAt(15)

      assert(manager.put(k1, v) == 30)
      assert(manager.put(k2, v) == 45)
    }

    "Get the value from cluster" in {
      val manager = new ClusterManager

      assert(manager.put(k1, v) == 15)

      manager.get(k1) shouldBe(Some(v))
    }

    "Get None from cluster if value does not exist" in {
      val manager = new ClusterManager

      assert(manager.put(k1, v) == 15)

      manager.get(k2) shouldBe(None)
    }
  }
}
