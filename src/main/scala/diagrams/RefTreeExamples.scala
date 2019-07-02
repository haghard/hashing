package diagrams

//https://github.com/stanch/reftree/blob/master/immutability.md
object RefTreeExamples {

  import reftree.core.ToRefTree
  import reftree.render._
  import reftree.diagram._
  import java.nio.file.Paths
  import Diagram.{sourceCodeCaption ⇒ diagram}

  case class Tree(value: Int, children: List[Tree])

  /*implicit val treeDerivationConfig = (ToRefTree.DerivationConfig[Tree]
    .rename("MyTree") // display as “MyTree”
    .tweakField("size", _.withName("s")) // label the field “s”, instead of “size”
    .tweakField("value", _.withTreeHighlight(true)) // highlight the value
    .tweakField("children", _.withoutName)) // do not label the “children” field
   */

  implicit val config = ToRefTree.DerivationConfig[Tree].rename("tree").tweakField("children", _.withoutName)

  val tree = Tree(1, Tree(2, Tree(3, Tree(4, Nil) :: Nil) :: Nil) :: Tree(7, Nil) :: Nil)

  //val tree = implicitly[ToRefTree[Tree]]

  val renderer = Renderer(renderingOptions = RenderingOptions(density = 75), directory = Paths.get("images"))

  renderer.render("tree", Diagram(tree))

  object A {
    import reftree.contrib.SimplifiedInstances.string
    implicit val personConfig = ToRefTree.DerivationConfig[Person].tweakField("firstName", _.withName("."))

    val renderer = Renderer(renderingOptions = RenderingOptions(density = 75), directory = Paths.get("images"))
    import renderer._

    case class Person(key: String, age: Int)
    val bob = Person("haghard", 33)
    diagram(bob).render("haghard")
  }

  object Projects {
    case class Project(id: String)
    case class Tenant(id: String, projects: List[Project])
    //val tb3 = Tenant("tenant.1", Vector(Project("tenant.1.project.1"), Project("tenant.1.project.2"), Project("tenant.1.project.3")))
    val tb3 = Tenant(
      "tenant.1",
      Project("tenant.1.project.1") :: Project("tenant.1.project.2") :: Project("tenant.1.project.3") :: Nil
    )

    import reftree.contrib.SimplifiedInstances.{list, seq}
    import reftree.contrib.OpticInstances._

    val renderer = Renderer(renderingOptions = RenderingOptions(density = 20), directory = Paths.get("images"))
    import renderer._

    diagram(tb3).render("tb3")
  }

  object Words {
    case class ShardEntity(id: String)
    case class Shard(id: String, item: List[ShardEntity])

    import reftree.contrib.SimplifiedInstances.{list, seq}
    val shards = Shard("a", ShardEntity("aa") :: ShardEntity("ab") :: ShardEntity("ac") :: Nil) ::
      Shard("b", ShardEntity("ba") :: ShardEntity("bb") :: ShardEntity("bc") :: Nil) :: Nil

    val renderer = Renderer(renderingOptions = RenderingOptions(density = 100), directory = Paths.get("images"))
    import renderer._
    diagram(shards).render("shards")
  }
}
