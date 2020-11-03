require('dotenv/config');
const sql = require('mssql');
const axios = require('axios');
const contentful = require('contentful-management');
const contentfulDelivery = require('contentful');
const { parseHtml } = require('contentful-html-rich-text-converter');

let client;
let deliveryClient;
const spaceId = process.env.CONTENTFUL_SPACE_ID;

(async function () {
  client = contentful.createClient({
    accessToken: process.env.CONTENTFUL_MANAGEMENT_ACCESS_TOKEN,
  });

  deliveryClient = contentfulDelivery.createClient({
    space: spaceId,
    accessToken: process.env.CONTENTFUL_DELIVERY_ACCESS_TOKEN,
  });

  sql.on('error', err => {
    console.log('error', err);
  });

  try {
    await sql.connect({
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      server: process.env.DB_SERVER,
      database: process.env.DB_NAME,
    });
  } catch (err) {
    console.log('sql connect error', err);
  }

  // await sendAuthors();
  // await convertToRichText();
  // await printAuthor();
  // await sendAuthors();
  // await createCategories();
  // await sendPosts();
  // await archiveUnlinkedAssets();
  // await updatePostsWithoutCategories();
  // await sendAuthorSlugs();
  // await postBodyContains('http://www.recruiter.com/recruitment-technology-trends-2014.pdf');
})();

const mimeTypes = {
  jpeg: 'image/jpeg',
  jpg: 'image/jpeg',
  png: 'image/png',
};

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function imageExists(url) {
  try {
    let res = await axios.get(url);
    return res.status === 200;
  } catch (error) {
    return false;
  }
}

async function getCategories() {
  const space = await client.getSpace(spaceId);
  const environment = await space.getEnvironment('master');
  const entries = await environment.getEntries({
    content_type: 'blogCategory',
  });
  return entries.items.map(i => ({
    id: i.sys.id,
    name: i.fields.name['en-US'],
    slug: i.fields.slug['en-US'],
  }));
}

async function createCategories() {
  const values = [
    ['case-studies', 'Case Studies'],
    ['client-stories', 'Client Stories'],
    ['employer-branding', 'Employer Branding'],
    ['facebook-recruiting', 'Facebook Recruiting'],
    ['link-love', 'Link Love'],
    ['product-updates', 'Product Updates'],
    ['resources', 'Resources'],
    ['social-recruiting', 'Social Recruiting'],
    ['welcome-to-jobcast', 'Welcome to Jobcast'],
    ['white-papers', 'White Papers'],
  ];

  values.forEach(async category => {
    await sleep(300);
    client
      .getSpace(spaceId)
      .then(space => space.getEnvironment('master'))
      .then(environment =>
        environment.createEntry('blogCategory', {
          fields: {
            slug: {
              'en-US': category[0],
            },
            name: {
              'en-US': category[1],
            },
          },
        }),
      )
      .then(async entry => entry.publish())
      .then(async entry => {
        const authorUpdateResult = await sql.query`
          UPDATE [dbo].[Authors]
            SET [ContentfulId] = ${entry.sys.id}
          WHERE [Login] = ${author.Login}
        `;
        console.log(author.login, authorUpdateResult);
      });
  });
}

async function printAuthor() {
  client
    .getSpace(spaceId)
    .then(space => space.getEnvironment('master'))
    .then(environment =>
      environment.getEntry('15jwOBqpxqSAOy2eOO4S0m'),
    )
    .then(entry => console.log(entry));
}

async function sendAuthors() {
  // be careful with this. need to add pauses, otherwise:
  // "Rate limit error occurred. Waiting for 1543 ms before retrying..."
  try {
    const result = await sql.query`
      SELECT [Id]
          ,[Login]
          ,[Email]
          ,[Name]
      FROM [dbo].[Authors] as a
      where exists (select 1 from dbo.Posts where Creator = a.Login) and [ContentfulId] IS NULL
      ORDER BY [Login]`;

    result.recordset.forEach(async author => {
      client
        .getSpace(spaceId)
        .then(space => space.getEnvironment('master'))
        .then(environment =>
          environment.createEntry('person', {
            fields: {
              name: {
                'en-US': author.Name,
              },
              email: {
                'en-US': author.Email,
              },
            },
          }),
        )
        .then(async entry => entry.publish())
        .then(async entry => {
          const authorUpdateResult = await sql.query`
            UPDATE [dbo].[Authors]
              SET [ContentfulId] = ${entry.sys.id}
            WHERE [Login] = ${author.Login}
          `;
          console.log(author.login, authorUpdateResult);
        });
    });
  } catch (err) {
    console.log('sql read', err);
  }
}

async function sendAuthorSlugs() {
  const result = await sql.query`
      SELECT [Id]
          ,[Login]
          ,[Email]
          ,[Name]
          ,ContentfulId
      FROM [dbo].[Authors] as a
      where ContentfulId is not null
      ORDER BY [Login]`;

  const space = await client.getSpace(spaceId);
  const environment = await space.getEnvironment('master');
  try {
    for (const a of result.recordset) {
      console.log('author', a.Login);

      const author = await environment.getEntry(a.ContentfulId);
      author.fields.slug = {
        'en-US':
          'author/' +
          a.Login.toLowerCase()
            .replace(/\s/g, '-')
            .replace(/@/g, '')
            .replace(/\./g, '-'),
      };
      const updatedAuthor = await author.update();
      await updatedAuthor.publish();
      console.log('author', a.Login);
    }
  } catch (error) {
    console.log('error', error);
  }
}

async function sendPosts(asDraft) {
  // asDraft == true is for sending jobs a second time when the first time failed because of validation reasons
  // asDraft == true will not send post body and will leave the post in draft mode

  // be careful with this. need to add pauses, otherwise:
  // "Rate limit error occurred. Waiting for 1543 ms before retrying..."

  const categories = await getCategories();

  const posts = await sql.query`
      SELECT p.[Id]
          ,a.ContentfulId
          ,p.[Title]
          ,p.[Link]
          ,p.[Name]
          ,p.[PubDate]
          ,p.[Status]
          ,p.[PostType]
          ,p.[Excerpt]
          ,p.[Content]
          ,p.[Creator]
          ,p.[RichText]
      FROM [dbo].[Posts] as p
        INNER JOIN Authors as a on a.Login = p.Creator
      where p.RichText IS NOT NULL
      ORDER BY [PubDate]`;

  for (let i = 0; i < posts.recordset.length; i++) {
    const post = posts.recordset[i];
    const slug = post.Link.replace(
      'http://www.jobcast.net/',
      '',
    ).replace(/\/$/g, '');

    const space = await client.getSpace(spaceId);
    const environment = await space.getEnvironment('master');

    console.log(`${slug}: processing`);
    await sleep(500);
    if (await postExists(environment, slug)) {
      console.log(`${slug}: already exists - skipping`);
      continue;
    }

    const postCategories = await sql.query`
          SELECT [PostId]
              ,[Domain]
              ,[Value]
          FROM [dbo].[Categories]
          where Domain = 'category' and PostId = ${post.Id}`;

    const data = {
      fields: {
        title: {
          'en-US': post.Title,
        },
        slug: {
          'en-US': slug,
        },
        author: {
          'en-US': {
            sys: {
              type: 'Link',
              linkType: 'Entry',
              id: post.ContentfulId,
            },
          },
        },
        publishDate: {
          'en-US': post.PubDate,
        },
        categories: {
          'en-US': postCategories.recordset.map(pc => ({
            sys: {
              type: 'Link',
              linkType: 'Entry',
              id: categories.find(c => c.name === pc.Value).id,
            },
          })),
        },
      },
    };

    if (!asDraft)
      data.fields.body = {
        'en-US': await cleansRichText(
          JSON.parse(post.RichText),
          slug,
        ),
      };

    await sleep(500);
    const hero = await uploadHeroImage(post.Id, slug);
    if (hero) {
      data.fields.heroImage = {
        'en-US': {
          sys: {
            type: 'Link',
            linkType: 'Asset',
            id: hero.sys.id,
          },
        },
      };
    } else {
      data.fields.heroImage = {
        'en-US': {
          sys: {
            type: 'Link',
            linkType: 'Asset',
            id: 'Js4bjmLXIjfPArvIiwWdn', // default image
          },
        },
      };
    }

    console.log(`${slug}: saving blog`);
    await sleep(500);

    try {
      const entry = await environment.createEntry('blogPost', data);

      if (asDraft) {
        console.log(`${slug}: blog saved complete: ${entry.sys.id}`);
      } else {
        console.log(`${slug}: publishing blog`);
        await sleep(500);
        await entry.publish();
        console.log(
          `${slug}: blog publish complete: ${entry.sys.id}`,
        );
      }
    } catch (error) {
      console.log(`${slug}: ***ERROR*** Error saving blog`);
    }
  }
}

async function postExists(environment, slug) {
  const response = await environment.getEntries({
    content_type: 'blogPost',
    'fields.slug': slug,
  });
  return !!response.items.length;
}

async function cleansRichText(body, slug) {
  let sections = [];
  for (let i = 0; i < body.content.length; i++) {
    let section = body.content[i];

    // there has to be a nodeType
    if (!section.nodeType && section.content) {
      sections.push(
        ...(await cleansRichText({ content: section.content }, slug))
          .content,
      );
      continue;
    }

    // contentful doesn't allow text at the root, so wrap in paragraph
    if (section.nodeType === 'text') {
      sections.push({
        data: {},
        nodeType: 'paragraph',
        content: [section],
      });
      continue;
    }

    // embedded-asset-block isn't allowed to be hyperlinked...strangely
    if (section.nodeType === 'hyperlink') {
      sections.push({
        data: {},
        nodeType: 'paragraph',
        content: [
          {
            ...section,
            content: section.content.filter(
              c => c.nodeType !== 'embedded-asset-block',
            ),
          },
        ],
      });
      // put the assets at the end
      sections.push(
        ...section.content.filter(
          c => c.nodeType === 'embedded-asset-block',
        ),
      );
      continue;
    }

    if (
      section.nodeType === 'unordered-list' ||
      section.nodeType === 'ordered-list'
    ) {
      section.content = section.content.filter(
        c => !(c.nodeType === 'text' && c.value.trim() === ''),
      );

      section.content.forEach(c => {
        if (c.nodeType !== 'list-item') return;

        // hyperlink must be wrapped in paragraph, even within a list-item
        for (let i = 0; i < c.content.length; i++) {
          const item = c.content[i];
          if (item.nodeType !== 'hyperlink') continue;

          c.content[i] = {
            data: {},
            nodeType: 'paragraph',
            content: [item],
          };
        }
      });
    }

    sections.push(section);
  }

  sections = removeDoubleHyperlink(sections);
  await uploadAndConvertAssets(sections, slug);

  body.content = sections;
  return body;
}

function removeDoubleHyperlink(content) {
  if (!content) return undefined;
  return content.map(c => {
    if (c.nodeType !== 'hyperlink')
      return {
        ...c,
        content: removeDoubleHyperlink(c.content),
      };

    if (c.content.length !== 1)
      return {
        ...c,
        content: removeDoubleHyperlink(c.content),
      };

    if (!c.content || c.content[0].nodeType !== 'hyperlink')
      return {
        ...c,
        content: removeDoubleHyperlink(c.content),
      };

    return {
      ...c.content[0],
      content: removeDoubleHyperlink(c.content[0].content),
    };
  });
}

async function uploadAndConvertAssets(content, slug) {
  for (let i = 0; i < content.length; i++) {
    const c = content[i];
    if (c.nodeType === 'embedded-asset-block')
      if (c.data.target.sys.type === 'Asset') {
        if (await imageExists(c.data.target.fields.file.url)) {
          console.log(
            `${slug}: uploading ${c.data.target.fields.file.url}`,
          );
          const space = await client.getSpace(spaceId);
          const environment = await space.getEnvironment('master');
          const asset = await environment.createAsset({
            fields: {
              title: {
                'en-US': `Blog ${slug}`,
              },
              description: {
                'en-US': c.data.target.fields.description,
              },
              file: {
                'en-US': {
                  contentType: c.data.target.fields.file.contentType,
                  fileName: c.data.target.fields.file.fileName,
                  upload: c.data.target.fields.file.url,
                },
              },
            },
          });

          await asset.processForAllLocales();
          const processedAsset = await environment.getAsset(
            asset.sys.id,
          );
          await processedAsset.publish();

          c.data.target = {
            sys: {
              id: asset.sys.id,
              type: 'Link',
              linkType: 'Asset',
            },
          };
        } else {
          console.log(
            `${slug}: attempted to upload ${c.data.target.fields.file.url} but doesn't exist`,
          );
          c.data.target = {
            sys: {
              id: '68qzkHjCboFfCsSxV2v9S6', // image not found
              type: 'Link',
              linkType: 'Asset',
            },
          };
        }
      }

    if (c.content) await uploadAndConvertAssets(c.content, slug);
  }
}

async function uploadHeroImage(postId, postSlug) {
  // to upload image from filesystem, use this: https://www.contentful.com/blog/2017/03/02/uploading-files-directly-to-contentful/

  const result = await sql.query`
  SELECT [PostId]
      ,[PostParent]
      ,[Link]
      ,[Title]
      ,[PostType]
      ,[AttachmentUrl]
  FROM [JobcastWordpress].[dbo].[FeaturedMedias]
  where PostParent = ${postId}`;

  if (!result.recordset.length) {
    console.log(`${postSlug}: hero image doesn't exist`);
    return;
  }

  const media = result.recordset[0];

  if (!(await imageExists(media.AttachmentUrl))) {
    console.log(
      `${postSlug}: hero image ${media.AttachmentUrl} not found`,
    );
    return;
  }

  const space = await client.getSpace(spaceId);
  const environment = await space.getEnvironment('master');
  const asset = await environment.createAsset({
    fields: {
      title: {
        'en-US': `Hero ${postSlug}`,
      },
      description: {
        'en-US': media.AttachmentUrl.replace(/^.*[\\\/]/, ''),
      },
      file: {
        'en-US': {
          contentType:
            mimeTypes[
              media.AttachmentUrl.substr(
                media.AttachmentUrl.lastIndexOf('.') + 1,
              )
            ], // https://stackoverflow.com/a/680949/188740
          fileName: media.AttachmentUrl.replace(/^.*[\\\/]/, ''), // https://stackoverflow.com/a/423385/188740
          upload: media.AttachmentUrl,
        },
      },
    },
  });
  await asset.processForAllLocales();

  const processedAsset = await environment.getAsset(asset.sys.id);
  await processedAsset.publish();
  console.log(
    `${postSlug}: uplaoded hero image ${media.AttachmentUrl}`,
  );
  return asset;
}

async function convertToRichText() {
  try {
    const result = await sql.query`
          SELECT [Id]
          ,[Title]
          ,[Link]
          ,[Name]
          ,[PubDate]
          ,[Status]
          ,[PostType]
          ,[Excerpt]
          ,[Content]
          ,[Creator]
      FROM [dbo].[Posts]
      WHERE [RichText] IS NULL
      ORDER BY [PubDate]`;

    for (let i = 0; i < result.recordset.length; i++) {
      const post = result.recordset[i];
      console.log(post.Link);
      const richText = parseHtml(post.Content);
      await sql.query`
          UPDATE [dbo].[Posts]
            SET [RichText] = ${JSON.stringify(richText)}
          WHERE [Id] = ${post.Id}
        `;
    }
  } catch (err) {
    console.log('sql read', err);
  }
}

async function archiveUnlinkedAssets(skip) {
  try {
    // https://www.contentful.com/faq/apis/#how-to-find-entries-assets-that-are-not-linked-to-any-entry
    const assetIds = await getAssetIds();
    for (const assetId of assetIds) {
      const entry = await deliveryClient.getEntries({
        links_to_asset: assetId, // Important: b/c we're using the content delivery API, only published linked entries are returned, so if an entry is linked but in draft, it won't return here. Do not archive those.
      });
      if (entry.total === 0) {
        const space = await client.getSpace(spaceId);
        const environment = await space.getEnvironment('master');
        let a = await environment.getAsset(assetId);
        a = await a.unpublish();
        a = await a.archive();
      }
    }
  } catch (error) {
    console.log('error', error);
  }
}

async function getAssetIds(skip) {
  const assets = await deliveryClient.getAssets({
    order: 'sys.createdAt',
    skip: skip || 0,
  });
  return assets.items
    .map(i => i.sys.id)
    .concat(
      assets.skip + assets.limit < assets.total
        ? await getAssetIds(assets.skip + assets.limit)
        : [],
    );
}

async function updatePostsWithoutCategories() {
  const categories = await getCategories();

  const posts = await getPostsWithoutCategory();

  for (const post of posts) {
    await sleep(1000);
    const slug = post.fields.slug['en-US'];
    const postCategories = await sql.query`
    SELECT c.[PostId]
      ,c.[Domain]
      ,c.[Value]
    FROM [dbo].[Categories] as c
    inner join dbo.Posts as p on p.Id = c.PostId
    where c.Domain = 'category' and p.Link = ${
      'http://www.jobcast.net/' + slug + '/'
    }`;

    if (!postCategories.recordset.length) {
      console.log(slug, 'No categories');
      continue;
    }

    post.fields.categories = {
      'en-US': postCategories.recordset.map(pc => ({
        sys: {
          type: 'Link',
          linkType: 'Entry',
          id: categories.find(c => c.name === pc.Value).id,
        },
      })),
    };

    try {
      const updatedPost = await post.update();
      await updatedPost.publish();
      console.log(
        slug,
        'set categories to: ' +
          postCategories.recordset.map(pc => pc.Value).join(' | '),
      );
    } catch (error) {
      console.log(`${slug} error`, error);
    }
  }
}

async function postBodyContains(text) {
  const posts = await getAllPosts();
  for (const post of posts) {
    if (
      post.fields.body &&
      JSON.stringify(post.fields.body['en-US'].content).includes(text)
    )
      console.log(post.sys.id, post.fields.slug['en-US']);
  }
}

async function getPostsWithoutCategory(skip) {
  const space = await client.getSpace(spaceId);
  const environment = await space.getEnvironment('master');
  const posts = await environment.getEntries({
    order: 'sys.createdAt',
    skip: skip || 0,
    content_type: 'blogPost',
    'fields.categories[exists]': false,
  });
  return posts.items.concat(
    posts.skip + posts.limit < posts.total
      ? await getPostsWithoutCategory(posts.skip + posts.limit)
      : [],
  );
}

async function getAllPosts(skip) {
  const space = await client.getSpace(spaceId);
  const environment = await space.getEnvironment('master');
  const posts = await environment.getEntries({
    order: 'sys.createdAt',
    skip: skip || 0,
    content_type: 'blogPost',
  });
  return posts.items.concat(
    posts.skip + posts.limit < posts.total
      ? await getAllPosts(posts.skip + posts.limit)
      : [],
  );
}

async function getPostBySlug(slug) {
  const space = await client.getSpace(spaceId);
  const environment = await space.getEnvironment('master');
  const posts = await environment.getEntries({
    order: 'sys.createdAt',
    content_type: 'blogPost',
    'fields.slug': slug,
  });

  return posts.items[0];
}
