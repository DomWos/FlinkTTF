import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroSerializer {
    byte[] serializeAvro(SpecificRecord[] records, SpecificDatumWriter writer) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, (BinaryEncoder)null);
        SpecificRecord[] var5 = records;
        int var6 = records.length;

        for(int var7 = 0; var7 < var6; ++var7) {
            SpecificRecord rec = var5[var7];
            writer.write(rec, encoder);
        }

        encoder.flush();
        return outputStream.toByteArray();
    }
}
