# Sequential
for type in {1..5}; do
  for der in {0..12}; do
    echo "SEQ: Derivability: $der, Type: $type"
    ./gradlew --stop && ./gradlew run --args="0 0 $der $type" > seq\_0\_$der\_$type.txt
    echo "floating by the docks" | sudo -S service postgresql restart
  done
done

# Hybrid
for type in {1..5}; do
  for cache in {0..9}; do
    for der in {0..12}; do
      echo "HYB: Derivability: $der, Cache: $cache, Type: $type"
      ./gradlew --stop && ./gradlew run --args="1 $cache $der $type" > hyb\_$cache\_$der\_$type.txt
      echo "floating by the docks" | sudo -S service postgresql restart
    done
  done
done

# Batch
for type in {1..5}; do
  for der in {0..12}; do
    echo "BAT: Derivability: $der, Type: $type"
    ./gradlew --stop && ./gradlew run --args="2 0 $der $type" > bat\_0\_$der\_$type.txt
    echo "floating by the docks" | sudo -S service postgresql restart
  done
done

# MVR
for type in {1..5}; do
  for cache in {0..9}; do
    for der in {0..12}; do
      echo "MVR: Derivability: $der, Cache: $cache, Type: $type"
      ./gradlew --stop && ./gradlew run --args="3 $cache $der $type" > mvr\_$cache\_$der\_$type.txt
      echo "floating by the docks" | sudo -S service postgresql restart
    done
  done
done