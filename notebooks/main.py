import polars_bio as pb
# from polars_bio.quality_control_op import base_sequence_content
# from polars_bio.quality_control_viz import plot_base_content

# fastq_path = "./ERR194147.fastq"
fastq_path = "./example.fastq"

pb.ctx.set_option("datafusion.execution.target_partitions", "2")
pb.ctx.set_option("datafusion.execution.batch_size", "10")

fastq_results = pb.base_sequence_content(fastq_path)

print(f"Processed FASTQ file directly from path: {fastq_path}")
print("Base content analysis results (first 10 positions):")
print(fastq_results.head(10))

max_a = fastq_results["a_count"].max() if "a_count" in fastq_results.columns else 0
max_t = fastq_results["t_count"].max() if "t_count" in fastq_results.columns else 0
max_g = fastq_results["g_count"].max() if "g_count" in fastq_results.columns else 0
max_c = fastq_results["c_count"].max() if "c_count" in fastq_results.columns else 0
max_n = fastq_results["n_count"].max() if "n_count" in fastq_results.columns else 0

print(f"\nMaximum counts - A: {max_a}, T: {max_t}, G: {max_g}, C: {max_c}, N: {max_n}")

# pb.plot_base_content(
#     fastq_results, 
#     figsize=(14, 8), 
#     title='Base Distribution in FASTQ Sequences'
# )
