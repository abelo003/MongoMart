<!DOCTYPE html>
<html lang="en">

    <head>
        <meta charset="utf-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <meta name="description" content="">
        <meta name="author" content="">
        <link rel="icon" href="/img/favicon.ico" type="image/x-icon" />
        <link rel="shortcut icon" href="/img/favicon.ico" type="image/x-icon" />

        <title>${item.title} - MongoMart</title>

        <!-- Bootstrap Core CSS -->
        <link href="/css/bootstrap.min.css" rel="stylesheet">

        <!-- Custom CSS -->
        <link href="/css/shop-homepage.css" rel="stylesheet">

        <!-- HTML5 Shim and Respond.js IE8 support of HTML5 elements and media queries -->
        <!-- WARNING: Respond.js doesn't work if you view the page via file:// -->
        <!--[if lt IE 9]>
        <script src="https://oss.maxcdn.com/libs/html5shiv/3.7.0/html5shiv.js"></script>
        <script src="https://oss.maxcdn.com/libs/respond.js/1.4.2/respond.min.js"></script>
        <![endif]-->
    </head>

<body>

<#-- Include navigation -->
<#include "includes/nav.ftl">

<!-- Page Content -->
<div class="container">

    <div class="row">

        <div class="row">
            <div class="col-md-12">
                <ol class="breadcrumb">
                    <li><a href="/">Home</a></li>
                    <li><a href="/?category=${item.category}">${item.category}</a></li>
                    <li class="active">${item.title}</li>
                </ol>
            </div>
        </div>

        <!-- Product Item Heading -->
        <div class="row">
            <div class="col-lg-12">
                <h1 style="margin-top: 0px;" class="page-header">${item.title}
                    <small>${item.slogan}</small>
                </h1>
            </div>
        </div>
        <!-- /.row -->

        <!-- Product Item Row -->
        <div class="row">

            <div class="col-md-8">
                <img class="img-responsive" src="${item.img_url}" alt="">
            </div>

            <div class="col-md-4">
                <h3>Product Description</h3>

                <div class="ratings" style="padding-left: 0px;">
                    <p class="pull-right">${item.num_reviews} reviews</p>
                    <p>

                        <#-- Display stars -->
                        <#assign stars=5>
                        <#list 1..stars as i>
                            <#if item.stars gte i>
                                <span class="glyphicon glyphicon-star"></span>
                            <#else>
                                <span class="glyphicon glyphicon-star-empty"></span>
                            </#if>
                        </#list>

                    </p>
                </div>

                <p>
                    ${item.description}
                </p>

                <a class="btn btn-primary" href="/cart/add?itemid=${item.id}">Add to cart <span class="glyphicon glyphicon-chevron-right"></span></a>

            </div>

        </div>
        <!-- /.row -->

        <!-- Related Products Row -->
        <div class="row">

            <div class="col-lg-12">
                <h3 class="page-header">Reviews</h3>
            </div>

            <div class="col-lg-12">

                <#list item.reviews as review>
                    <!-- Comment -->
                    <div>
                        <div>
                            <h4 class="media-heading">${review.name}
                                <small>${review.date?date}</small>
                            </h4>
                            <div class="ratings" style="padding-left: 0px;">

                                <#-- Display stars -->
                                <#assign stars=5>
                                <#list 1..stars as i>
                                    <#if review.stars gte i>
                                        <span class="glyphicon glyphicon-star"></span>
                                    <#else>
                                        <span class="glyphicon glyphicon-star-empty"></span>
                                    </#if>
                                </#list>

                            </div>

                            ${review.comment}
                        </div>
                    </div>

                    <hr />

                </#list>

            </div>

        </div>

        <!-- Comments Form -->
        <div class="well">
            <h4>Add a Review:</h4>
            <form action="/add-review" role="form">
                <input type="hidden" name="itemid" value="${itemid}" />
                <div class="form-group">
                    <label for="review">Review:</label>
                    <textarea name="review" class="form-control" rows="3"></textarea>
                </div>
                <div class="form-group">
                    <label for="name">Name:</label>
                    <input type="text" class="form-control" id="name" name="name" placeholder="Enter display name">
                </div>
                <div class="form-group">

                    <label class="radio-inline">
                        <input type="radio" name="stars" id="stars" value="1"> 1 star
                    </label>
                    <label class="radio-inline">
                        <input type="radio" name="stars" id="stars" value="2"> 2 star
                    </label>
                    <label class="radio-inline">
                        <input type="radio" name="stars" id="stars" value="3"> 3 star
                    </label>
                    <label class="radio-inline">
                        <input type="radio" name="stars" id="stars" value="4"> 4 star
                    </label>
                    <label class="radio-inline">
                        <input type="radio" name="stars" id="stars" value="5" checked> 5 star
                    </label>
                </div>
                <button type="submit" class="btn btn-primary">Submit Review</button>
            </form>
        </div>

        <hr>

        <!-- Related Products Row -->
        <div class="row">

            <div class="col-lg-12">
                <h3 class="page-header">Related Products</h3>
            </div>

            <#list related_items as related_item>
                <div class="col-sm-3 col-xs-6">
                    <a href="/item?id=${related_item.id}">
                        <img class="img-responsive portfolio-item" src="${related_item.img_url}" alt="">
                    </a>
                </div>
            </#list>

        </div>
        <!-- /.row -->

    </div>

</div>

<#-- Include footer -->
<#include "includes/footer.ftl">

</body>

</html>
