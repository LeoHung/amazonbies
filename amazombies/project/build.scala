import sbt._
import Keys._
import org.scalatra.sbt._
import org.scalatra.sbt.PluginKeys._
import com.mojolly.scalate.ScalatePlugin._
import ScalateKeys._
import sbtassembly.Plugin._
import sbtassembly.Plugin.AssemblyKeys._

object AmazombiesBuild extends Build {
  val Organization = "com.example"
  val Name = "amazombies"
  val Version = "0.0.1"
  val ScalaVersion = "2.11.1"
  val ScalatraVersion = "2.3.0"

  // settings for sbt-assembly plugin
  val myAssemblySettings = assemblySettings ++ Seq(

    // handle conflicts during assembly task
    mergeStrategy in assembly <<= (mergeStrategy in assembly) {
      (old) => {
        case "about.html" => MergeStrategy.first
        case x => old(x)
      }
    },

    // copy web resources to /webapp folder
    resourceGenerators in Compile <+= (resourceManaged, baseDirectory) map {
      (managedBase, base) =>
        val webappBase = base / "src" / "main" / "webapp"
        for {
          (from, to) <- webappBase ** "*" x rebase(webappBase, managedBase / "main" / "webapp")
        } yield {
          Sync.copy(from, to)
          to
        }
    }
  )

  lazy val project = Project (
    "amazombies",
    file("."),
    settings = ScalatraPlugin.scalatraWithJRebel ++ scalateSettings ++ Seq(
      organization := Organization,
      name := Name,
      version := Version,
      scalaVersion := ScalaVersion,
      resolvers += Classpaths.typesafeReleases,
      libraryDependencies ++= Seq(
        "org.scalatra" %% "scalatra" % ScalatraVersion,
        "org.scalatra" %% "scalatra-scalate" % ScalatraVersion,
        "org.scalatra" %% "scalatra-specs2" % ScalatraVersion % "test",
        "ch.qos.logback" % "logback-classic" % "1.1.2" % "runtime",
        "org.eclipse.jetty" % "jetty-webapp" % "9.1.5.v20140505" % "container;provided;compile",
        "org.eclipse.jetty" % "jetty-plus" % "9.1.5.v20140505" % "container",
        "javax.servlet" % "javax.servlet-api" % "3.1.0",
        "com.github.cb372" %% "scalacache-memcached" % "0.4.0"
      ),
      scalateTemplateConfig in Compile <<= (sourceDirectory in Compile){ base =>
        Seq(
          TemplateConfig(
            base / "webapp" / "WEB-INF" / "templates",
            Seq.empty,  /* default imports should be added here */
            Seq(
              Binding("context", "_root_.org.scalatra.scalate.ScalatraRenderContext", importMembers = true, isImplicit = true)
            ),  /* add extra bindings here */
            Some("templates")
          )
        )
      }
    )
  ).settings(myAssemblySettings:_*)
}
