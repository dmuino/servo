/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.servo.publish.atlas;

import com.fasterxml.jackson.core.JsonGenerator;
import com.netflix.servo.Metric;
import com.netflix.servo.tag.Tag;
import com.netflix.servo.tag.TagList;
import com.netflix.servo.util.Objects;
import com.netflix.servo.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A Request sent to the atlas-publish API.
 */
public final class UpdateRequest implements JsonPayload {

  private final TagList tags;
  private final List<AtlasMetric> metrics;

  public UpdateRequest(TagList tags, Metric[] metricsToSend, int numMetrics, long step) {
    Preconditions.checkArgument(metricsToSend.length > 0, "metricsToSend is empty");
    Preconditions.checkArgument(numMetrics > 0 && numMetrics <= metricsToSend.length,
        "numMetrics is 0 or out of bounds");

    this.metrics = new ArrayList<AtlasMetric>(numMetrics);
    for (int i = 0; i < numMetrics; ++i) {
      Metric m = metricsToSend[i];
      if (m.hasNumberValue()) {
        metrics.add(new AtlasMetric(m, step));
      }
    }

    this.tags = tags;
  }

  TagList getTags() {
    return tags;
  }

  List<AtlasMetric> getMetrics() {
    return metrics;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof UpdateRequest)) {
      return false;
    }
    UpdateRequest req = (UpdateRequest) obj;
    return tags.equals(req.getTags())
        && metrics.equals(req.getMetrics());
  }

  @Override
  public int hashCode() {
    return Objects.hash(tags, metrics);
  }

  @Override
  public String toString() {
    return "UpdateRequest{tags=" + tags + ", metrics=" + metrics + '}';
  }

  @Override
  public void toJson(JsonGenerator gen) throws IOException {
    gen.writeStartObject();

    // common tags
    gen.writeObjectFieldStart("tags");
    for (Tag tag : tags) {
      gen.writeStringField(
          ValidCharacters.toValidCharset(tag.getKey()),
          ValidCharacters.toValidCharset(tag.getValue()));
    }
    gen.writeEndObject();

    gen.writeArrayFieldStart("metrics");
    for (AtlasMetric m : metrics) {
      m.toJson(gen);
    }
    gen.writeEndArray();

    gen.writeEndObject();
    gen.flush();
  }
}
