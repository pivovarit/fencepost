package com.pivovarit.fencepost;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HeadersCodecTest {

    @Test
    void shouldRoundTripSimpleHeaders() {
        Map<String, String> headers = Map.of("key", "value", "foo", "bar");
        String json = HeadersCodec.toJson(headers);
        Map<String, String> result = HeadersCodec.fromJson(json);
        assertThat(result).isEqualTo(headers);
    }

    @Test
    void shouldHandleNullAndEmptyMaps() {
        assertThat(HeadersCodec.toJson(null)).isNull();
        assertThat(HeadersCodec.toJson(Map.of())).isNull();
        assertThat(HeadersCodec.fromJson(null)).isEmpty();
        assertThat(HeadersCodec.fromJson("")).isEmpty();
    }

    @Test
    void shouldRoundTripHeadersWithSpecialJsonCharacters() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("key-with-\"quotes\"", "value-with-\\backslash");
        String json = HeadersCodec.toJson(headers);
        Map<String, String> result = HeadersCodec.fromJson(json);
        assertThat(result).isEqualTo(headers);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\n", "\t", "\r", "\0"})
    void shouldRejectControlCharactersInHeaderKeys(String controlChar) {
        Map<String, String> headers = Map.of("bad" + controlChar + "key", "value");
        assertThatThrownBy(() -> HeadersCodec.toJson(headers))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Header key")
          .hasMessageContaining("control character");
    }

    @ParameterizedTest
    @ValueSource(strings = {"\n", "\t", "\r", "\0"})
    void shouldRejectControlCharactersInHeaderValues(String controlChar) {
        Map<String, String> headers = Map.of("key", "bad" + controlChar + "value");
        assertThatThrownBy(() -> HeadersCodec.toJson(headers))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Header value")
          .hasMessageContaining("control character");
    }

    @Test
    void shouldAcceptPrintableUnicodeInHeaders() {
        Map<String, String> headers = Map.of("Content-Type", "application/json; charset=utf-8", "X-Emoji", "😀");
        String json = HeadersCodec.toJson(headers);
        Map<String, String> result = HeadersCodec.fromJson(json);
        assertThat(result).isEqualTo(headers);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\uD800", "\uDFFF", "bad\uD800x", "\uDC00leading-low"})
    void shouldRejectLoneSurrogatesInHeaderKeys(String loneSurrogate) {
        Map<String, String> headers = Map.of(loneSurrogate, "value");
        assertThatThrownBy(() -> HeadersCodec.toJson(headers))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Header key")
          .hasMessageContaining("surrogate");
    }

    @ParameterizedTest
    @ValueSource(strings = {"\uD800", "\uDFFF", "bad\uD800x", "\uDC00leading-low"})
    void shouldRejectLoneSurrogatesInHeaderValues(String loneSurrogate) {
        Map<String, String> headers = Map.of("key", loneSurrogate);
        assertThatThrownBy(() -> HeadersCodec.toJson(headers))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Header value")
          .hasMessageContaining("surrogate");
    }

    @Test
    void shouldAcceptWellFormedSurrogatePairs() {
        Map<String, String> headers = Map.of("astral", "😀");
        String json = HeadersCodec.toJson(headers);
        Map<String, String> result = HeadersCodec.fromJson(json);
        assertThat(result).isEqualTo(headers);
    }
}
