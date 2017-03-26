package yamrcraft.etlite.pipeline
import com.typesafe.config.Config
import yamrcraft.etlite.transformers.ProtoTransformer

class GenericProtoPipelineFactory extends AbstractProtoPipelineFactory {
  override def transformer(config: Config): ProtoTransformer = new ProtoTransformer(config)
}
