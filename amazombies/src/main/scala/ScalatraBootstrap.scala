import cmu.amazombies._
import org.scalatra._
import javax.servlet.ServletContext

class ScalatraBootstrap extends LifeCycle {
  override def init(context: ServletContext) {
    val servlet = new AmazombiesServerlet
    servlet.warmUpQ1Map
    context.mount(servlet, "/*")
  }
}
