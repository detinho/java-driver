/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.data;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.List;

public class ValuesHelper {

  public static ByteBuffer[] encodeValues(
      Object[] values,
      List<DataType> fieldTypes,
      CodecRegistry codecRegistry,
      ProtocolVersion protocolVersion) {
    Preconditions.checkArgument(
        values.length <= fieldTypes.size(),
        "Too many values (expected %s, got %s)",
        fieldTypes.size(),
        values.length);

    ByteBuffer[] encodedValues = new ByteBuffer[fieldTypes.size()];
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      TypeCodec<Object> codec = codecRegistry.codecFor(fieldTypes.get(i), value);
      encodedValues[i] = codec.encode(value, protocolVersion);
    }
    return encodedValues;
  }
}
