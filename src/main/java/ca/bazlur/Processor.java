package ca.bazlur;

import ca.bazlur.model.DistanceMatrix;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class Processor {

  private static final ObjectMapper mapper = new ObjectMapper();
  public static final String DATA_FOLDER = "data";
  public static final String DISTANCE_MATRIX_CSV = "distance_matrix.csv";

  public static void main(String[] args) throws IOException {

    try (final var data = Files.list(Path.of(DATA_FOLDER))) {
      var matrix = prepareData(data);
      createCsv(matrix);
    }
  }

  private static Map<String, List<Edge>> prepareData(final Stream<Path> data) {
    var matrix = new HashMap<String, List<Edge>>();
    data
        .map(Processor::load)
        .map(Processor::calculateEdge)
        .flatMap(Collection::stream)
        .forEach(edge -> {
          if (!matrix.containsKey(edge.source())) {
            matrix.put(edge.source(), new ArrayList<>());
          }
          matrix.get(edge.source()).add(edge);
        });
    return matrix;
  }

  private static void createCsv(final Map<String, List<Edge>> matrix) throws IOException {
    final var entry = matrix.entrySet().stream().toList().get(0);
    entry.getValue().sort(Comparator.comparing(Edge::destination, Comparator.naturalOrder()));

    final var sources = new ArrayList<>(matrix.keySet());
    sources.sort(Comparator.naturalOrder());

    final var out = new FileWriter(Processor.DISTANCE_MATRIX_CSV);
    final var headers = new String[sources.size() + 1];
    headers[0] = "Origin";
    for (int i = 0; i < sources.size(); i++) {
      headers[i + 1] = sources.get(i);
    }

    try (var printer = new CSVPrinter(out,
        CSVFormat.DEFAULT.withHeader(headers))) {

      matrix.entrySet()
          .stream()
          .sorted(Entry.comparingByKey())
          .forEach((e) -> {
            final String[] row = creteCsvRow(headers, e);
            try {
              printer.printRecord((Object[]) row);
            } catch (IOException exception) {
              throw new RuntimeException(exception);
            }
          });
    }
  }

  private static String[] creteCsvRow(final String[] headers,
      final Entry<String, List<Edge>> entry) {
    final var row = new String[headers.length];
    final var key = entry.getKey();
    row[0] = key;
    for (int i = 1; i < row.length; i++) {
      final int finalI = i;
      final var any = entry.getValue().stream()
          .filter(edge -> edge.source().equals(key) && edge.destination().equals(headers[finalI]))
          .findAny();
      if (any.isPresent()) {
        row[i] = String.valueOf(any.get().distance());
      } else {
        row[i] = "0";
      }
    }
    return row;
  }

  private static List<Edge> calculateEdge(final DistanceMatrix distanceMatrix) {
    final var row = distanceMatrix.getRows().get(0);
    final var destinationAddresses = distanceMatrix.getDestinationAddresses();
    destinationAddresses.removeIf(String::isEmpty);

    final var elements = row.getElements();
    final var originAddresses = distanceMatrix.getOriginAddresses();
    final var origin = originAddresses.get(0);
    assert destinationAddresses.size() == elements.size();
    final var edges = new ArrayList<Edge>();

    for (int i = 0; i < destinationAddresses.size(); i++) {
      final var destination = destinationAddresses.get(i);
      final var element = elements.get(i);
      final var distance = element.getDistance();
      if (distance != null) {
        final var edge = new Edge(origin, destination, distance.getValue());
        edges.add(edge);
      }
    }

    return edges;
  }

  private static DistanceMatrix load(Path path) {
    try {
      return mapper.reader().readValue(path.toFile(), DistanceMatrix.class);
    } catch (IOException e) {
      throw new RuntimeException("Unable to process: " + path, e);
    }
  }
}
