streamvis create-field start-time float "Start time"
streamvis create-field experiment-name string "Name of the experiment"
streamvis create-field noisy-channel-epsilon float "Probability of mutating an emitted symbol"
streamvis create-field with-BOS-token bool "Whether the generating process uses a BOS token"

streamvis create-field sgd_step int "SGD step"
streamvis create-field context_start int "Starting position of LLM context probe"
streamvis create-field context_slice_size int "Size of a slice of LLM context probe"
streamvis create-field layer_slice_index int "NN layer slice index"
streamvis create-field cev_dimension int "Dimension in latent space corresponding to the CEV value"
streamvis create-field cev_value float "Cumulative explained variance"

streamvis create-series cev-curve cev_dimension cev_value \
  sgd_step context_start context_slice_size \
  layer_slice_index noisy_channel_epsilon

streamvis create-field cev_threshold float "Threshold for computing number of dimensions (in [0, 1])"
streamvis create-field participation_ratio float "Participation ratio (metric for dimension measurement)"
streamvis create-field effective_rank float "Effective Rank"

streamvis create-series cev-metric cev_threshold participation_ratio effective_rank \
  sgd_step context_start context_slice_size \
  layer_slice_index noisy_channel_epsilon

