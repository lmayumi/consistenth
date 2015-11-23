package com.hash

import scala.util.hashing.MurmurHash3

trait Key
trait Value

case class ClusterManager() {
  var cluster = List((Node(), 15), (Node(), 30), (Node(), 45), (Node(), 60))
  var partition = 60

  private def getNode(index: Int): (Node, Int) = {
    cluster.filter {
      case ((_ , range)) => range >= index
    }.head
  }
  def put(k: Key, v: Value): Int = {
    val (node, index) = getNode(hash(k))
    node.put(k, v)
    index
  }
  def get(k: Key): Option[Value] = {
    val (node, _) = getNode(hash(k))
    node.get(k)
  }
  def hash(k: Key): Int = {
    MurmurHash3.stringHash(k.toString) % partition
  }

  def addNodeAt(index: Int): Boolean = {
    cluster = ((Node(), index) :: cluster).sortWith {
      case ((_, index1), (_, index2)) => index1 < index2
    }
    true
  }

  def removeNodeAt(index: Int): Boolean = {
    cluster = cluster.filterNot {
      case ((_, i)) => i == index
    }
    true
  }

  case class Node() {
    var map = Map[Key, Value]()
    def put(k: Key, v: Value): Boolean = {
      map = map + (k -> v)
      true
    }
    def get(k: Key): Option[Value] = {
      map.get(k)
    }
  }
}
