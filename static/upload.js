const fileInput = document.getElementById('fileInput');
const uploadBtn = document.getElementById('uploadBtn');
const progressContainer = document.getElementById('progress-container');
const progressBar = document.getElementById('progress-bar');
const statusText = document.getElementById('status-text');

fileInput.addEventListener('change', () => {
  uploadBtn.disabled = !fileInput.files.length;
  statusText.innerHTML = '';
  progressBar.style.width = '0%';
  progressContainer.style.display = 'none';
});

uploadBtn.addEventListener('click', () => {
  if (!fileInput.files.length) return;

  const file = fileInput.files[0];
  if (file.type !== 'text/csv' && !file.name.endsWith('.csv')) {
    statusText.textContent = '❌ Error: Please upload a valid CSV file.';
    return;
  }

  const formData = new FormData();
  formData.append('emailFile', file);

  const xhr = new XMLHttpRequest();
  xhr.open('POST', '/upload', true);

  xhr.upload.onprogress = (event) => {
    if (event.lengthComputable) {
      const percent = (event.loaded / event.total) * 100;
      progressBar.style.width = percent + '%';
      progressContainer.style.display = 'block';
      statusText.textContent = `📤 Uploading: ${Math.round(percent)}%`;
    }
  };

  xhr.onload = () => {
    uploadBtn.disabled = false;

    if (xhr.status === 200) {
      progressBar.style.width = '100%';
      const response = JSON.parse(xhr.responseText);

      statusText.innerHTML = `✅ Upload complete!<br><a href="${response.file_url}" target="_blank">📄 View file: ${response.filename}</a>`;

      if (response.threads && response.threads.length > 0) {
        statusText.innerHTML += `<br><br><strong>🧵 Top Email Threads:</strong>`;
        response.threads.forEach(thread => {
          statusText.innerHTML += `
            <div style="margin-top: 10px; text-align: left;">
              <b>Subject:</b> ${thread.Subject}<br>
              <b>From:</b> ${thread.From}<br>
              <b>To:</b> ${thread.To}<br>
              <b>Date:</b> ${thread.Date}<br>
              <hr style="border: 0.5px solid #ccc;">
            </div>`;
        });
      }
    } else {
      statusText.textContent = `❌ Upload failed: ${xhr.statusText}`;
    }
  };

  xhr.onerror = () => {
    statusText.textContent = '❌ Upload error occurred.';
    uploadBtn.disabled = false;
  };

  uploadBtn.disabled = true;
  statusText.textContent = '🚀 Starting upload...';
  xhr.send(formData);
});
