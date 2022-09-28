package ca.bazlur;

import static java.time.Duration.ofSeconds;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Downlaoder {


  public static final String URL = "https://maps.googleapis.com/maps/api/distancematrix/json?origins=%s&destinations=%s&key=%s";
  public static final String API_KEY = "[YOUR__MAP__API__KEY]";  //configure it from google
  private static final HttpClient httpClient = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(ofSeconds(10))
      .build();

  public static void main(String[] args)
      throws IOException, InterruptedException {

    var map = new LinkedHashMap<String, List<String>>();

    final var cities = Files.readAllLines(Path.of("city.txt"))
        .stream()
        //.limit(5)
        .map(Downlaoder::stripCityOnly)
        .toList();

    for (final String city : cities) {
      for (final String s : cities) {
        if (!map.containsKey(city)) {
          map.put(city, new ArrayList<>());
        }
        if (!city.equals(s)) {
          map.get(city).add(s);
        }
      }
    }

    map.entrySet().stream()
        //.limit(1)
        .forEach((entry) -> {
          final var origin = entry.getKey();
          final var destinations = chopped(entry.getValue(), 24);

          int counter = 0;
          for (final List<String> destination : destinations) {
            try {
              sendRequest(origin + "_" + ++counter + "_output.json", origin,
                  (String.join("|", destination)));
            } catch (URISyntaxException | IOException | InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        });

  }

  private static void sendRequest(String fileName,
      final String origin, final String destination)
      throws URISyntaxException, IOException, InterruptedException {
    final var fullUrl = (String.format(Downlaoder.URL, encodeValue(origin),
        encodeValue(destination),
        Downlaoder.API_KEY));
    final var request = HttpRequest.newBuilder(
            new URI(fullUrl))
        .GET()
        .build();

    System.out.println(request.uri());
    HttpResponse<String> response = httpClient.send(request,
        HttpResponse.BodyHandlers.ofString());
    final var body = response.body();

    Files.writeString(Path.of(fileName), body);
  }

  private static String encodeValue(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  static <T> List<List<T>> chopped(List<T> list, final int L) {
    List<List<T>> parts = new ArrayList<>();
    final int N = list.size();
    for (int i = 0; i < N; i += L) {
      parts.add(new ArrayList<T>(
          list.subList(i, Math.min(N, i + L)))
      );
    }
    return parts;
  }

  private static String stripCityOnly(final String line) {
    final var s = line.trim().split("\\s");
    if (s.length > 2) {
      return s[0] + " " + s[1];
    }
    return s[0];
  }
}
