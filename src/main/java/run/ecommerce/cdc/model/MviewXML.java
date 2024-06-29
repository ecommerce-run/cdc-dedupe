package run.ecommerce.cdc.model;

import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

@Component
public class MviewXML {

    public Map<String, Map<String, List<String>>> storage;
    public Map<String, String> indexerMap;

    public MviewXML() {
        storage = new HashMap<>();
        indexerMap = new HashMap<>();
    }

    public void init(String rootDirectoryPath) {

        var rootDir = new File(rootDirectoryPath);

        Stream<File> allMviewFiles = Stream.concat(
                findMviewFilesRecursive(rootDir, "app"),
                findMviewFilesRecursive(rootDir, "vendor")
        );

        var tables = allMviewFiles
            .flatMap(file -> {
                List<Element[]> storage = new ArrayList<>();
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

                DocumentBuilder builder = null;
                Document doc = null;
                try {
                    builder = factory.newDocumentBuilder();
                    doc = builder.parse(file);
                } catch (ParserConfigurationException | IOException | SAXException e) {
                    throw new RuntimeException(e);
                }

                var rootNode = doc.getFirstChild();
                while (!rootNode.getNodeName().equals("config")) {
                    rootNode = rootNode.getNextSibling();
                }

                for (var viewNode = rootNode.getFirstChild();
                     viewNode != null; viewNode = viewNode.getNextSibling()) {
                    if (!viewNode.getNodeName().equals("view")) continue;
                    Element viewElement = (Element) viewNode;
                    var subscriptionsNode = viewNode.getFirstChild();
                    while (subscriptionsNode != null && !subscriptionsNode.getNodeName().equals("subscriptions")) {
                        subscriptionsNode = subscriptionsNode.getNextSibling();
                    }
                    if (subscriptionsNode == null) continue;
                    for (var tableNode = subscriptionsNode.getFirstChild();
                         tableNode != null;
                         tableNode = tableNode.getNextSibling()) {
                        if (!tableNode.getNodeName().equals("table")) continue;
                        var tableElement = (Element) tableNode;
                        storage.add(new Element[]{tableElement, viewElement});
                    }
                }
                return storage.stream();
            });
        for (var table : tables.toList()) {
            var source = table[0];
            var target = table[1];
            var sourceTableName = source.getAttribute("name");
            var sourceTableColumn = source.getAttribute("entity_column");
            var targetIndexerName = target.getAttribute("id");
            var targetIndexerClass = target.getAttribute("class");
            storage.putIfAbsent(sourceTableName, new HashMap<>());
            storage.get(sourceTableName).putIfAbsent(sourceTableColumn, new ArrayList<>());
            storage.get(sourceTableName).get(sourceTableColumn).add(targetIndexerName);
            indexerMap.put(targetIndexerName, targetIndexerClass);
        }
    }

    protected static Stream<File> findMviewFilesRecursive(File dir, String subDir) {
        File subdirDir = new File(dir, subDir);
        return Stream.of(subdirDir.listFiles())
                .filter(Objects::nonNull)
                .flatMap(file -> file.isDirectory()
                        ? findMviewFilesRecursive(file, "")
                        : file.getName().endsWith("mview.xml") ? Stream.of(file) : Stream.empty()
                );
    }
}