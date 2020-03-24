package ai

import bbb.avro.dto.RatesDTO

package object humn {
 type ValueorWatermark = Either[(String, Long), RatesDTO]
}
