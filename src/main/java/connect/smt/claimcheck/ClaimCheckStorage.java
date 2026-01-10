package connect.smt.claimcheck;

import java.util.Map;

public interface ClaimCheckStorage {

  void configure(Map<String, ?> configs);

  String store(String key, byte[] data);

  void close();
}
