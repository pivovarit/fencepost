package com.pivovarit.fencepost;

import java.util.LinkedHashMap;
import java.util.Map;

final class HeadersCodec {

    private HeadersCodec() {
    }

    static String toJson(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            first = false;
            sb.append(jsonString(entry.getKey())).append(":").append(jsonString(entry.getValue()));
        }
        sb.append("}");
        return sb.toString();
    }

    static Map<String, String> fromJson(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        Map<String, String> result = new LinkedHashMap<>();
        String content = json.strip();
        if (!content.startsWith("{") || !content.endsWith("}")) {
            return Map.of();
        }
        content = content.substring(1, content.length() - 1).strip();
        if (content.isEmpty()) {
            return Map.of();
        }
        int i = 0;
        while (i < content.length()) {
            i = skipWhitespace(content, i);
            if (content.charAt(i) != '"') {
                break;
            }
            int[] keyResult = readString(content, i);
            String key = content.substring(i + 1, keyResult[0]);
            i = skipWhitespace(content, keyResult[1]);
            if (i >= content.length() || content.charAt(i) != ':') {
                break;
            }
            i = skipWhitespace(content, i + 1);
            if (content.charAt(i) != '"') {
                break;
            }
            int[] valResult = readString(content, i);
            String value = content.substring(i + 1, valResult[0]);
            i = skipWhitespace(content, valResult[1]);
            result.put(unescape(key), unescape(value));
            if (i < content.length() && content.charAt(i) == ',') {
                i++;
            }
        }
        return result;
    }

    private static int skipWhitespace(String s, int i) {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
        }
        return i;
    }

    private static int[] readString(String s, int start) {
        int i = start + 1;
        while (i < s.length()) {
            if (s.charAt(i) == '\\') {
                i += 2;
            } else if (s.charAt(i) == '"') {
                return new int[]{i, i + 1};
            } else {
                i++;
            }
        }
        return new int[]{i, i};
    }

    private static String unescape(String s) {
        if (s.indexOf('\\') == -1) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '\\' && i + 1 < s.length()) {
                sb.append(s.charAt(++i));
            } else {
                sb.append(s.charAt(i));
            }
        }
        return sb.toString();
    }

    private static String jsonString(String value) {
        if (value == null) {
            return "null";
        }
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
