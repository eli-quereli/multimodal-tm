from bertopic import BERTopic
from bertopic.representation import VisualRepresentation
from bertopic.backend import MultiModalBackend
import torch
from transformers import pipeline


def save_results(topic_model, model_version, docs=None, images=None):
    # Save results
    topic_info = topic_model.get_topic_info()
    topic_info.to_csv(f"../results/{model_version}_topic_info.csv", index=False)
    if docs:
        docs_topic_info = topic_model.get_document_info(docs)
        docs_topic_info.to_csv(f"../results/{model_version}_docs_topic_info.csv")
    else:
        docs_topic_info = topic_model.get_document_info(images)
        docs_topic_info.to_csv(f"../results/{model_version}_docs_topic_info.csv")
    print(f"{len(topic_info)} topics found.")
    # save model using safetensors
    topic_model.save(f"../models/{model_version}", serialization="safetensors", save_ctfidf=True)


def text_topic_modeling(docs, umap_model, min_topic_size, model_version):
    # Train topic model with texts only
    topic_model = BERTopic(
        language='multilingual',
        umap_model=umap_model,
        min_topic_size=min_topic_size
    )
    # Train model
    topics, probs = topic_model.fit_transform(docs) 
    # Save results
    save_results(topic_model=topic_model, model_version=model_version, docs=docs)


def image_topic_modeling(images, umap_model, min_topic_size, model_version):
    # Specify the device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    # Image embedding model
    embedding_model = MultiModalBackend('clip-ViT-B-32', batch_size=32)
    # Load the image to text model on the specified device
    # image_to_text_model = "nlpconnect/vit-gpt2-image-captioning"
    image_to_text_model = pipeline("image-to-text", model="nlpconnect/vit-gpt2-image-captioning", device=0 if device == "cuda" else -1)
    # Pass the model to VisualRepresentation
    representation_model = {
        "Visual_Aspect": VisualRepresentation(image_to_text_model=image_to_text_model)
    }
    # Train our model with images only
    topic_model = BERTopic(
        embedding_model=embedding_model,
        representation_model=representation_model,
        umap_model=umap_model,
        min_topic_size=min_topic_size
    )
    topics, probs = topic_model.fit_transform(documents=None, images=images)
    # Save results
    save_results(topic_model=topic_model, model_version=model_version, images=images)


def multimodal_topic_modeling(docs, images, umap_model, min_topic_size, model_version):
    # Image embedding model
    embedding_model = MultiModalBackend('clip-ViT-B-32', batch_size=32)
    # Image to text representation model
    # image_to_text_model = "nlpconnect/vit-gpt2-image-captioning"
    image_to_text_model = pipeline("image-to-text", model="nlpconnect/vit-gpt2-image-captioning", device=0 if device == "cuda" else -1)
    representation_model = {
        "Visual_Aspect": VisualRepresentation(image_to_text_model=image_to_text_model)
    }
    # Train our model with images only
    topic_model = BERTopic(
        embedding_model=embedding_model, 
        representation_model=representation_model,
        umap_model=umap_model,
        min_topic_size=min_topic_size
    )
    topics, probs = topic_model.fit_transform(documents=docs, images=images)
    # Save results
    save_results(topic_model=topic_model, model_version=model_version, docs=docs, images=images)