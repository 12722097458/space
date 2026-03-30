package com.ityj.feishu.bitable.client;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.Map;

/**
 * 基于 {@link RestTemplate} 的通用 HTTP 调用，供业务接口封装复用。
 */
@Component
public class RemoteHttpClient {

    private final RestTemplate restTemplate;

    public RemoteHttpClient(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    /**
     * GET：返回响应体（2xx）；非 2xx 或网络错误时抛出 {@link IllegalStateException}。
     */
    public String get(URI uri, Map<String, String> headers) {
        HttpEntity<Void> entity = new HttpEntity<>(toHeaders(headers));
        return exchange(uri, HttpMethod.GET, entity);
    }

    /**
     * 通用请求：可自定义方法、请求体与头。
     */
    public String exchange(URI uri, HttpMethod method, HttpEntity<?> requestEntity) {
        try {
            ResponseEntity<String> response = restTemplate.exchange(uri, method, requestEntity, String.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                throw new IllegalStateException(
                        "HTTP " + response.getStatusCode() + " uri=" + uri + " body=" + response.getBody());
            }
            return response.getBody() != null ? response.getBody() : "";
        } catch (RestClientException ex) {
            throw new IllegalStateException("远程请求失败 uri=" + uri + ": " + ex.getMessage(), ex);
        }
    }

    private static HttpHeaders toHeaders(Map<String, String> headers) {
        HttpHeaders h = new HttpHeaders();
        if (headers != null) {
            headers.forEach(h::set);
        }
        return h;
    }
}
